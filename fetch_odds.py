import os
import requests
from supabase import create_client, client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def cet_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))

event_created = {}  
market_event = {}  

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

def post_to_slack(message: str):
    if not SLACK_WEBHOOK:
        print("No Slack webhook defined")
        return
    requests.post(SLACK_WEBHOOK, json={"text": message})



url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: client = create_client(url, key)


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
    if now_cet - event_created[event_id] > timedelta(hours=24):
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

    market_event[market_id] = event_id

    return market_id

def insert_odds(market_id, side, price, max_limit):

    if market_id not in market_event:
        return

    event_id = market_event[market_id]

    if event_id not in event_created:
        return

    
    if cet_now() - event_created[event_id] > timedelta(hours=24):
        return

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
            "pulled_at": cet_now().isoformat()
        }).execute()


def process_event(events):
    upsert_event(events)

    for event in events["events"]:
        for key, period in event.get("periods", {}).items():

            if period.get("money_line") and period.get("number") == 0:

                market_id = upsert_market(event, period)

                if not market_id:
                    continue  

                for side, line in period["money_line"].items():
                    insert_odds(market_id, side, line, 0)


url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
querystring = {"league_ids": "1842,2438,199868,200813,1740,217401,217399,212572,212576,217400", "event_type": "prematch", "sport_id": "1", "is_have_odds": "true"}
headers = {
    "x-rapidapi-key": "67356f377fmsh90217b51616e9d8p11c494jsnf87faa9470db",
    "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
event_list = response.json()

process_event(event_list)
