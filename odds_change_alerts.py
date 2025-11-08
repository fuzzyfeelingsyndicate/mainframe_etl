import os
import pandas as pd
import requests
from supabase import create_client,  client
from datetime import datetime, timedelta
from tabulate import tabulate

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

supabase: client = create_client(url, key)

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')


def post_to_slack(data):
    if not SLACK_WEBHOOK:
        print("No Slack webhook defined")
        return
    
    if len(data) == 0:
        print("empty data")
        return
    
    if isinstance(data, pd.DataFrame):
        message = f"```\n{tabulate(data, headers='keys', tablefmt='grid')}\n```"
    else:
        message = str(data)
    
    payload = {"text": message}
    resp = requests.post(SLACK_WEBHOOK, json=payload)
    if resp.status_code != 200:
        print("Slack post failed:", resp.status_code, resp.text)



def find_value_corrected(row):
    overound_cal = round(100 / (100 / row['price_home'] + 100 / row['price_away'] + 100 / row['price_draw'])*100, 2)
    overound =  (100 / row['price_home']) + (100 / row['price_draw']) + (100 / row['price_away'])
    no_vig = (overound_cal / row['price_home']) + (overound_cal / row['price_draw']) + (overound_cal / row['price_away'])
    home_no_vig = round((overound_cal / row['price_home']), 2)
    draw_no_vig = round(overound_cal / row['price_draw'] ,2)
    away_no_vig = round(overound_cal / row['price_away'], 2)

    return pd.Series({
        'overround': overound,
        'no_vig': no_vig,
        'home_no_vig' : home_no_vig,
        'draw_no_vig' : draw_no_vig,
        'away_no_vig' : away_no_vig
    })


def check_odds(timedel = 2):
     url = os.getenv("SUPABASE_URL")
     key = os.getenv("SUPABASE_KEY")
     supabase: client = create_client(url, key)
     cutoff_date = datetime.now() - timedelta(hours=timedel)
     
     events = pd.DataFrame(supabase.table('events').select('*').execute().data)
     markets = pd.DataFrame(supabase.table('markets').select('*').execute().data)
     odds_history = pd.DataFrame(supabase.table('odds_history').select('*').gt('pulled_at', cutoff_date.isoformat()).execute().data)

     if odds_history.empty:
         post_to_slack(odds_history)
         return
     else:
         match_winner_odds_home = odds_history[odds_history['side']=='home'][['market_id', 'price', 'max_limit', 'pulled_at']]
         match_winner_odds_away = odds_history[odds_history['side']=='away'][['market_id', 'price']]
         match_winner_odds_draw = odds_history[odds_history['side']=='draw'][['market_id', 'price']]

         homeXaway = match_winner_odds_home.merge(match_winner_odds_away, suffixes=('_home', '_away'),how = 'left', on='market_id')
         match_winner =  homeXaway.merge(match_winner_odds_draw, how = 'left', on='market_id').rename(
             columns={'price':'price_draw'})[['market_id','price_home','price_draw','price_away','max_limit','pulled_at']]
         
         match_winner[['overround', 'no_vig' , 'home_no_vig', 'draw_no_vig', 'away_no_vig']] = match_winner.apply(find_value_corrected, axis=1)
         Ix2 = match_winner.merge(markets[['market_id', 'event_id']].merge(events, how='left', on='event_id'),
                   how='left', on='market_id')[['event_id', 'league_name', 'starts', 'price_home', 'price_draw', 'price_away', 'max_limit', 'overround', 'no_vig', 'home_no_vig', 'draw_no_vig', 'away_no_vig', 'market_id', 'pulled_at']]
         
         Ix2.sort_values( by=['event_id', 'pulled_at'], inplace = True, ascending = [True, True])

         Ix2['no_vig_diff'] =  Ix2.groupby('event_id')['home_no_vig'].diff()

         Ix2.sort_values(by = ['event_id', 'pulled_at'], inplace = True, ascending = [True, False])

         result_df = Ix2[['event_id', 'league_name', 'starts', 'home_no_vig', 'draw_no_vig', 'away_no_vig', 'no_vig_diff', 'pulled_at']]
         result_df['no_vig_diff'] = result_df['no_vig_diff'].fillna(0)
         result_df = result_df[(result_df['no_vig_diff']<0) & result_df['no_vig_diff']>0]

         supabase.table('match_winner').delete().neq('event_id', 0).execute()         
         supabase.table('match_winner').insert(result_df.to_dict('records')).execute()
         post_to_slack(result_df)
         return 


check_odds(timedel = 2)
