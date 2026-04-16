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

    for event in events['events']:

        event_id = event["event_id"]

        existing = (
            supabase.table("events")
            .select("event_id", "created_at")
            .eq("event_id", event_id)
            .execute()
        )

        is_new = not existing.data

        if is_new:
            created_at_cet = cet_now()

            supabase.table("events").insert({
                "event_id": event_id,
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
                "created_at": created_at_cet.isoformat()
            }).execute()

            event_created[event_id] = created_at_cet

            post_to_slack(
                f":sparkles: New event added: *{event['home']}* vs *{event['away']}* in *{event['league_name']}* — starts {event['starts']}"
            )

        else:
            created_at_str = existing.data[0]["created_at"]
            event_created[event_id] = datetime.fromisoformat(created_at_str)

            supabase.table("events").update({
                "sport_id": event["sport_id"],
                "league_id": event["league_id"],
                "league_name": event["league_name"],
                "starts": event["starts"],
                "home_team": event["home"],
                "away_team": event["away"],
            }).eq("event_id", event_id).execute()


def upsert_market(event, period):

    event_id = event["event_id"]

    if event_id not in event_created:
        return None

    now_cet = cet_now()

    if now_cet - event_created[event_id] > MAX_EVENT_AGE:
        return None

    market_type = "money_line"
    parameter = 0
    line_id = period["line_id"]
    period_number = period["number"]

    existing = (
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
        "created_at": now_cet.isoformat()
    }

    if existing.data:
        market_id = existing.data[0]["market_id"]
        supabase.table("markets").update(data).eq("market_id", market_id).execute()
    else:
        result = supabase.table("markets").insert(data).execute()
        market_id = result.data[0]["market_id"]

    return market_id

def insert_odds(event_id, market_id, side, price, max_limit):

    if event_id not in event_created:
        return

    if cet_now() - event_created[event_id] > MAX_EVENT_AGE:
        return

    existing = (
        supabase.table("odds_history")
        .select("price", "pulled_at")
        .eq("market_id", market_id)
        .eq("side", side)
        .order("pulled_at", desc=True)
        .limit(1)
        .execute()
    )

    if existing.data:
        last_price = existing.data[0]["price"]
        if last_price == price:
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

    for event in events["events"]:

        event_id = event["event_id"] 

        for key, period in event.get("periods", {}).items():

            if period.get("money_line") and period.get("number") == 0:

                market_id = upsert_market(event, period)

                if not market_id:
                    continue  

                for side, line in period["money_line"].items():
                    insert_odds(event_id, market_id, side, line, period.get("max_limit", 0))


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
