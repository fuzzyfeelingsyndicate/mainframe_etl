import os
import requests
from supabase import create_client, Client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def cet_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))

MAX_EVENT_AGE = timedelta(days=7)

event_created = {}

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

def post_to_slack(message: str):
    if not SLACK_WEBHOOK:
        print("No Slack webhook defined")
        return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": message}, timeout=10)
    except requests.RequestException as e:
        print(f"Slack notification failed: {e}")



supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)
rapid_url = os.getenv("RAPID_URL")
rapid_api_key = os.getenv("RAPID_API_KEY")
rapid_api_host = os.getenv("RAPID_API_HOST")


def upsert_event(events):
    all_ids = [e["event_id"] for e in events["events"]]

    existing_rows = (
        supabase.table("events")
        .select("event_id", "created_at")
        .in_("event_id", all_ids)
        .execute()
    ).data
    existing_map = {r["event_id"]: r["created_at"] for r in existing_rows}

    now_cet = cet_now()
    new_events = []

    for event in events["events"]:
        event_id = event["event_id"]
        if event_id not in existing_map:
            new_events.append({
                "event_id": event_id,
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
                "created_at": now_cet.isoformat()
            })
            event_created[event_id] = now_cet
            post_to_slack(
                f":sparkles: New event added: *{event['home']}* vs *{event['away']}* in *{event['league_name']}* — starts {event['starts']}"
            )
        else:
            event_created[event_id] = datetime.fromisoformat(existing_map[event_id])
            supabase.table("events").update({
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
            }).eq("event_id", event_id).execute()

    if new_events:
        supabase.table("events").insert(new_events).execute()


market_cache = {}

def load_markets(event_ids):
    rows = (
        supabase.table("markets")
        .select("market_id", "event_id", "line_id")
        .in_("event_id", event_ids)
        .eq("market_type", "money_line")
        .eq("parameter", 0)
        .execute()
    ).data
    for r in rows:
        market_cache[(r["event_id"], r["line_id"])] = r["market_id"]

def load_latest_odds(market_ids):
    if not market_ids:
        return {}
    cutoff = (cet_now() - timedelta(hours=24)).isoformat()
    rows = (
        supabase.table("odds_history")
        .select("market_id", "side", "price", "pulled_at")
        .in_("market_id", market_ids)
        .gt("pulled_at", cutoff)
        .order("pulled_at", desc=True)
        .execute()
    ).data
    seen = {}
    for r in rows:
        key = (r["market_id"], r["side"])
        if key not in seen:
            seen[key] = r["price"]
    return seen

def upsert_market(event, period):
    event_id = event["event_id"]
    if event_id not in event_created:
        return None
    if cet_now() - event_created[event_id] > MAX_EVENT_AGE:
        return None

    line_id = period["line_id"]
    cache_key = (event_id, line_id)

    if cache_key not in market_cache:
        data = {
            "event_id": event_id,
            "line_id": line_id,
            "period_number": period["number"],
            "market_type": "money_line",
            "parameter": 0,
            "created_at": cet_now().isoformat()
        }
        result = (
            supabase.table("markets")
            .upsert(data, on_conflict="event_id,line_id,market_type,parameter")
            .execute()
        )
        market_id = result.data[0]["market_id"]
        market_cache[cache_key] = market_id

    return market_cache[cache_key]

def insert_odds(event_id, market_id, side, price, max_limit, latest_odds):
    if event_id not in event_created:
        return
    if cet_now() - event_created[event_id] > MAX_EVENT_AGE:
        return
    if latest_odds.get((market_id, side)) == price:
        return

    supabase.table("odds_history").insert({
        "event_id": event_id,
        "market_id": market_id,
        "side": side,
        "price": price,
        "max_limit": max_limit,
        "pulled_at": cet_now().isoformat()
    }).execute()


def process_event(events):
    upsert_event(events)

    valid_ids = [e["event_id"] for e in events["events"] if e["event_id"] in event_created]
    load_markets(valid_ids)

    market_ids = list(market_cache.values())
    latest_odds = load_latest_odds(market_ids)

    for event in events["events"]:
        event_id = event["event_id"]
        for key, period in event.get("periods", {}).items():
            if period.get("money_line") and period.get("number") == 0:
                market_id = upsert_market(event, period)
                if not market_id:
                    continue
                for side, line in period["money_line"].items():
                    insert_odds(event_id, market_id, side, line, period.get("max_limit", 0), latest_odds)


if __name__ == "__main__":
    api_url = rapid_url
    querystring = {"league_ids": "2438,199868,200813,1740,217401,217399,212572,212576,217400,217562,199211,200201,1978,1952,1951", "event_type": "prematch", "sport_id": "1", "is_have_odds": "true"}
    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host
    }

    try:
        response = requests.get(api_url, headers=headers, params=querystring, timeout=30)
        response.raise_for_status()
        event_list = response.json()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        raise SystemExit(1)

    process_event(event_list)
