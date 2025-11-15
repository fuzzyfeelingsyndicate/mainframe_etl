import os
import psycopg2
import random 
import pandas as pd
import json
import requests
from supabase import create_client, client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# -----------------------------
# Supabase setup
# -----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# Slack helper
# -----------------------------
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
def post_to_slack(message: str):
    if not SLACK_WEBHOOK:
        return
    payload = {"text": message}
    requests.post(SLACK_WEBHOOK, json=payload)

# -----------------------------
# CET helper
# -----------------------------
def cet_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))

# -----------------------------
# Global caches
# -----------------------------
event_created = {}  # event_id -> start_time (datetime CET)
market_event = {}   # market_id -> event_id

# -----------------------------
# Freshness check (3 hours) using event start time
# -----------------------------
def is_event_fresh(event_start_iso: str):
    start_dt = datetime.fromisoformat(event_start_iso).astimezone(ZoneInfo("Europe/Berlin"))
    now = cet_now()
    return now - start_dt <= timedelta(hours=3)

# -----------------------------
# Upsert events
# -----------------------------
def upsert_event(events):
    for event in events['events']:
        existing = (
            supabase.table("events")
            .select("event_id", "created_at")
            .eq("event_id", event["event_id"])
            .execute()
        )

        is_new_event = not existing.data
        now_cet = cet_now()

        created_at = now_cet.isoformat() if is_new_event else existing.data[0]["created_at"]

        supabase.table("events").upsert({
            "event_id": event["event_id"],
            "sport_id": event["sport_id"],
            "league_id": event["league_id"],
            "league_name": event["league_name"],
            "starts": event["starts"],
            "home_team": event["home"],
            "away_team": event["away"],
            "created_at": created_at
        }).execute()

        # Cache the start time for freshness checks
        event_created[event["event_id"]] = datetime.fromisoformat(event["starts"]).astimezone(ZoneInfo("Europe/Berlin"))

        if is_new_event:
            msg = f":sparkles: New event added: *{event['home']}* vs *{event['away']}* in *{event['league_name']}* — starts {event['starts']}"
            post_to_slack(msg)

# -----------------------------
# Upsert market
# -----------------------------
def upsert_market(event, period):
    event_id = event["event_id"]

    # Check freshness using event start time
    event_start = event.get("starts")
    if not is_event_fresh(event_start):
        return None

    market_type = "money_line"
    parameter = 0
    line_id = period["line_id"]
    period_number = period["number"]

    existing_market = (
        supabase.table("markets")
        .select("market_id")
        .eq("event_id", event_id)
        .eq("line_id", line_id)
        .eq("market_type", market_type)
        .eq("parameter", parameter)
        .execute()
    )

    data = {
        "event_id": event_id,
        "line_id": line_id,
        "period_number": period_number,
        "market_type": market_type,
        "parameter": parameter,
        "created_at": cet_now().isoformat()
    }

    if existing_market.data:
        market_id = existing_market.data[0]["market_id"]
        supabase.table("markets").update(data).eq("market_id", market_id).execute()
    else:
        result = supabase.table("markets").insert(data).execute()
        market_id = result.data[0]["market_id"]

    # Cache market -> event for odds
    market_event[market_id] = event_id
    return market_id

# -----------------------------
# Insert odds
# -----------------------------
def insert_odds(market_id, side, price, max_limit=None):
    if market_id is None:
        return

    event_id = market_event.get(market_id)
    if not event_id:
        return

    event_start = event_created.get(event_id)
    if not event_start:
        return

    now_cet = cet_now()

    # Skip if event is older than 3 hours from start time
    if now_cet - event_start > timedelta(hours=3):
        return

    # Check for existing identical odds
    existing = (
        supabase.table("odds_history")
        .select("*")
        .eq("market_id", market_id)
        .eq("side", side)
        .eq("price", price)
        .eq("max_limit", max_limit)
        .order("pulled_at", desc=True)
        .limit(1)
        .execute()
    )

    if not existing.data:
        supabase.table("odds_history").insert({
            "market_id": market_id,
            "side": side,
            "price": price,
            "max_limit": max_limit,
            "pulled_at": now_cet.isoformat()
        }).execute()

# -----------------------------
# Process events from API
# -----------------------------
def process_event(events):
    upsert_event(events)

    for event in events['events']:
        for key, period in event.get('periods', {}).items():
            # Only moneyline full-time (period_number = 0)
            if period.get("money_line") and period.get("number") == 0:
                market_id = upsert_market(event, period)
                if market_id is None:
                    continue
                for side, line in period["money_line"].items():
                    insert_odds(market_id, side, line, 0)

# -----------------------------
# Fetch events from Pinnacle API
# -----------------------------
def fetch_event_list():
    url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
    querystring = {"league_ids":"1835,1842","event_type":"prematch","sport_id":"1","is_have_odds":"true"}
    headers = {
        "x-rapidapi-key": os.getenv("PINNACLE_KEY"),
        "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    event_list = fetch_event_list()
    process_event(event_list)
