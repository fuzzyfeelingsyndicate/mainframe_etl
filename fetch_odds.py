import os
import psycopg2
import random 
import pandas as pd
import json
import requests
from supabase import create_client,  client
from datetime import datetime


url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"

querystring = {
    "league_ids": "1835,1842",
    "event_type": "prematch",
    "sport_id": "1",
    "is_have_odds": "true"
}

headers = {
    "x-rapidapi-key": "67356f377fmsh90217b51616e9d8p11c494jsnf87faa9470db",
    "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
event_list = response.json()

# ---------------------------------------------------
# Slack helper
# ---------------------------------------------------
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

def post_to_slack(message: str):
    if not SLACK_WEBHOOK:
        return
    payload = {"text": message}
    requests.post(SLACK_WEBHOOK, json=payload)

# ---------------------------------------------------
# Supabase connection
# ---------------------------------------------------
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: client = create_client(url, key)

# ---------------------------------------------------
# In-memory caches for fast lookups
# ---------------------------------------------------
event_created = {}     # event_id -> datetime
market_event = {}      # market_id -> event_id

# ---------------------------------------------------
# Upsert event (always)
# ---------------------------------------------------
def upsert_event(events):
    for event in events['events']:

        existing = (
            supabase.table("events")
            .select("event_id, created_at")
            .eq("event_id", event["event_id"])
            .execute()
        )

        if existing.data:
            created_at = existing.data[0]["created_at"]
        else:
            created_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        # Upsert event
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

        # Cache created_at
        event_created[event["event_id"]] = datetime.fromisoformat(created_at)

        # Slack for new events
        if not existing.data:
            msg = (
                f":sparkles: New event added: *{event['home']}* vs *{event['away']}* "
                f"in *{event['league_name']}* — starts {event['starts']}"
            )
            post_to_slack(msg)

# ---------------------------------------------------
# Upsert market (only if event is ≤ 3 hours old)
# ---------------------------------------------------
def upsert_market(event_id, line_id, period_number, market_type, parameter):

    event_created_at = event_created.get(event_id)
    if not event_created_at:
        return None

    # Skip old events
    if datetime.utcnow() - event_created_at > timedelta(hours=3):
        return None

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
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    }

    if existing.data:
        market_id = existing.data[0]["market_id"]
        supabase.table("markets").update(data).eq("market_id", market_id).execute()
    else:
        result = supabase.table("markets").insert(data).execute()
        market_id = result.data[0]["market_id"]

    # Cache market -> event
    market_event[market_id] = event_id
    return market_id

# ---------------------------------------------------
# Insert odds (only if event is ≤ 3 hours old)
# ---------------------------------------------------
def insert_odds(market_id, side, price, max_limit=None):

    event_id = market_event.get(market_id)
    if not event_id:
        return

    event_created_at = event_created.get(event_id)
    if not event_created_at:
        return

    # Skip old events silently
    if datetime.utcnow() - event_created_at > timedelta(hours=3):
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
            "pulled_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        }).execute()

# ---------------------------------------------------
# Process events
# ---------------------------------------------------
def process_event(events):
    upsert_event(events)

    for event in events['events']:
        for key, period in event.get('periods', {}).items():

            # Moneyline only (period number = 0)
            if period.get('money_line') and period.get('number') == 0:

                market_id = upsert_market(
                    event['event_id'],
                    period['line_id'],
                    period['number'],
                    "money_line",
                    0
                )

                if market_id is None:
                    continue  # Skip odds if market skipped

                # Insert moneyline odds
                for side, line in period['money_line'].items():
                    insert_odds(market_id, side, line, 0)

process_event(event_list)



#1842
#1835



# sportid_random = random.randint(1,10000)
# leagueid_random = random.randint(1,10000)

# def main(id, leagueid):
#     try:
#         conn = psycopg2.connect(
#            user=os.getenv('SUPABASE_USER'),
#            password= os.getenv('SUPABASE_PASS'),
#            host=os.getenv('SUPABASE_HOST'),
#            port=6543,
#            dbname=os.getenv('SUPABASE_DB'),
#            client_encoding='utf8'
#         )
#         cur = conn.cursor()

#         # Check table structure first
#         cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'list_of_lagues_poapi'")
#         columns = cur.fetchall()
#         print("Available columns:", [col[0] for col in columns])
        
#         cur.execute('''insert into "list_of_leagues_poapi" (sportId, leagueId, name) 
#         values(%s, %s, %s)''', (id, leagueid, f'test{leagueid}'))   

#         conn.commit()
#         cur.close()
#         conn.close()
#         print('Success: Data inserted')
#     except Exception as e:
#         print(f'Error: {e}')

# if __name__ == "__main__":
#     main(sportid_random, leagueid_random)

