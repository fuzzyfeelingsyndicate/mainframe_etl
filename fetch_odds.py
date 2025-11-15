import os
import psycopg2
import random 
import pandas as pd
import json
import requests
from supabase import create_client, client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"

querystring = {"league_ids":"1835,1842","event_type":"prematch","sport_id":"1","is_have_odds":"true"}

headers = {
    "x-rapidapi-key": "67356f377fmsh90217b51616e9d8p11c494jsnf87faa9470db",
    "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
event_list = response.json()

event_created = {}
market_event = {}

# -----------------------------
# Slack helper
# -----------------------------
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

def post_to_slack(message: str):
    if not SLACK_WEBHOOK:
        print("No Slack webhook defined")
        return
    payload = {"text": message}
    resp = requests.post(SLACK_WEBHOOK, json=payload)
    if resp.status_code != 200:
        print("Slack post failed:", resp.status_code, resp.text)


# -----------------------------
# Supabase connection
# -----------------------------
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: client = create_client(url, key)


# -----------------------------
# CET now() helper
# -----------------------------
def cet_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))


# -----------------------------
# Event upsert (with CET timestamp)
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

        if is_new_event:
            # Insert event with CET created_at
            supabase.table("events").insert({
                "event_id": event["event_id"],
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
                "created_at": now_cet.isoformat()
            }).execute()

            msg = f":sparkles: New event added: *{event['home']}* vs *{event['away']}* in *{event['league_name']}* — starts {event['starts']}"
            post_to_slack(msg)

        else:
            # Event exists → do not update created_at
            supabase.table("events").update({
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
            }).eq("event_id", event["event_id"]).execute()


# -----------------------------
# Market upsert
# -----------------------------
def upsert_market(event, period):
    event_id = event["event_id"]

    # Fetch event to check created_at freshness
    existing = (
        supabase.table("events")
        .select("created_at")
        .eq("event_id", event_id)
        .execute()
    )

    if not existing.data:
        return None  # should not happen

    event_created_at = datetime.fromisoformat(existing.data[0]["created_at"])
    now_cet = cet_now()

    # Skip if event older than 3 hours
    if now_cet - event_created_at > timedelta(hours=3):
        return None

    # Continue normally
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

    return market_id


# -----------------------------
# Insert odds (with 3-hour CET rule)
# -----------------------------
def insert_odds(market_id, side, price, max_limit):

    if market_id is None:
        return

    # Get event_id from cache
    event_id = market_event.get(market_id)
    if not event_id:
        return

    # Get event.created_at from cache
    event_created_at = event_created.get(event_id)
    if not event_created_at:
        return

    now_cet = cet_now()

    # Skip if event older than 3 hours
    if now_cet - event_created_at > timedelta(hours=3):
        return

    # Check if these odds already exist
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
# Process event
# -----------------------------
def process_event(events):
    upsert_event(events)

    for event in events["events"]:
        for key, period in event.get("periods", {}).items():

            # Only process moneyline full-time
            if period.get("money_line") and period.get("number") == 0:

                market_id = upsert_market(event, period)

                if market_id is None:
                    continue  # event too old → skip

                for side, line in period["money_line"].items():
                    insert_odds(market_id, side, line, 0)


# -----------------------------
# Run
# -----------------------------
process_event(event_list)
