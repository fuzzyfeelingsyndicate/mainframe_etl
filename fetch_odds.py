import os
import psycopg2
import random 
import pandas as pd
import json
import requests
from supabase import create_client,  client
from datetime import datetime


url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"

querystring = {"league_ids":"1835,1842","event_type":"prematch","sport_id":"1","is_have_odds":"true"}

headers = {
	"x-rapidapi-key": "67356f377fmsh90217b51616e9d8p11c494jsnf87faa9470db",
	"x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
event_list = response.json()

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
# Event upsert
# -----------------------------
def upsert_event(events):
    for event in events['events']:
        # Check if event already exists
        existing = (
            supabase.table("events")
            .select("event_id")
            .eq("event_id", event["event_id"])
            .execute()
        )

        is_new_event = not existing.data  # True if event doesn't exist

        supabase.table("events").upsert({
            "event_id": event["event_id"],
            "sport_id": event["sport_id"],
            "league_id": event["league_id"],
            "league_name": event["league_name"],
            "starts": event["starts"],
            "home_team": event["home"],
            "away_team": event["away"],
            "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }).execute()

        # Notify Slack if new event
        if is_new_event:
            msg = f":sparkles: New event added: *{event['home']}* vs *{event['away']}* in *{event['league_name']}* — starts {event['starts']}"
            post_to_slack(msg)

# -----------------------------
# Market upsert
# -----------------------------
def upsert_market(event_id, line_id, period_number, market_type, parameter):
    data = {
        "event_id": event_id,
        "line_id": line_id,
        "period_number": period_number,
        "market_type": market_type,
        "parameter": parameter,
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    }

    result = supabase.table("markets").upsert(
        data,
        on_conflict=["event_id", "line_id", "market_type", "parameter"]
    ).execute()

    if result.data:
        return result.data[0]["market_id"]
    else:
        existing = (
            supabase.table("markets")
            .select("market_id")
            .eq("event_id", event_id)
            .eq("line_id", line_id)
            .eq("market_type", market_type)
            .eq("parameter", parameter)
            .execute()
        )
        return existing.data[0]["market_id"] if existing.data else None

# -----------------------------
# Insert odds
# -----------------------------
def insert_odds(market_id, side, price, max_limit=None):

    existing = supabase.table("odds_history").select("*").eq("market_id", market_id).eq("side", side).eq("price", price).eq("max_limit", max_limit).order("pulled_at", desc=True).limit(1) .execute()

    if not existing.data:
        supabase.table("odds_history").insert({
            "market_id": market_id,
            "side": side,
            "price": price,
            "max_limit": max_limit,
            "pulled_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }).execute()

# -----------------------------
# Process event
# -----------------------------
def process_event(events):
    upsert_event(events)
    for event in events['events']:
        for key, period in event.get('periods', {}).items():
            if period.get('money_line') is not None and period.get('number') == 0:
                market_id = upsert_market(
                    event['event_id'],
                    period['line_id'],
                    period['number'],
                    "money_line",
                    0
                )
                for side, line in period.get('money_line', {}).items():
                    insert_odds(market_id, side, line, 0)

# -----------------------------
# Run
# -----------------------------
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

