import os
import pandas as pd
import requests
from supabase import create_client, client
from datetime import datetime, timedelta

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
        messages = []
        for i in range(len(data)):
            row = data.iloc[i]
            event_id = row.get('event_id', '')
            league_name = row.get('league_name', '')
            home_team = row.get('home_team', '')
            away_team = row.get('away_team', '')
            
            home_move = row.get('home_total_move', 0)
            away_move = row.get('away_total_move', 0)
            price_home = row.get('price_home', '')
            price_draw = row.get('price_draw', '')
            price_away = row.get('price_away', '')

            msg = (
                f"Event ID: {event_id}\n"
                f"League: {league_name}\n"
                f"Home: {home_team}\n"
                f"Away: {away_team}\n"
                f"Home Move: {home_move:.2f}%\n"
                f"Away Move: {away_move:.2f}%\n"
                f"price home: {price_home:}\n"
                f"price draw: {price_draw:}\n"
                f"price away: {price_away:}\n"
                "-----------------------------"
            )
            messages.append(msg)

        message = "\n".join(messages)
    else:
        message = str(data)
    
    payload = {"text": message}
    resp = requests.post(SLACK_WEBHOOK, json=payload)
    if resp.status_code != 200:
        print("Slack post failed:", resp.status_code, resp.text)



def find_value_corrected_vectorized(df):
    ph = pd.to_numeric(df['price_home'], errors='coerce')
    pa = pd.to_numeric(df['price_away'], errors='coerce')
    pd_ = pd.to_numeric(df['price_draw'], errors='coerce')

    valid = ph.notna() & pa.notna() & pd_.notna()

    res = pd.DataFrame(index=df.index, columns=[
        'overround','no_vig','home_no_vig','draw_no_vig','away_no_vig'
    ])

    overround = 100/ph + 100/pd_ + 100/pa
    res.loc[valid, 'overround'] = overround[valid]

    over_cal = 100 / (100/ph + 100/pa + 100/pd_) * 100

    h_nv = over_cal / ph
    d_nv = over_cal / pd_
    a_nv = over_cal / pa

    res.loc[valid, 'home_no_vig'] = h_nv[valid].round(2)
    res.loc[valid, 'draw_no_vig'] = d_nv[valid].round(2)
    res.loc[valid, 'away_no_vig'] = a_nv[valid].round(2)
    res.loc[valid, 'no_vig'] = (h_nv + d_nv + a_nv)[valid]

    return res

def check_odds(timedel=60):
    cutoff_date = datetime.now() - timedelta(minutes=timedel)
    
    events = pd.DataFrame(supabase.table('events').select('*').execute().data)
    markets = pd.DataFrame(supabase.table('markets').select('*').execute().data)
    odds_history = pd.DataFrame(
        supabase.table('odds_history')
        .select('*')
        .gt('pulled_at', cutoff_date.isoformat())
        .execute().data
    )

    if odds_history.empty:
        post_to_slack(odds_history)
        return

    home = odds_history[odds_history['side']=='home'][['market_id','price','max_limit','pulled_at']]
    away = odds_history[odds_history['side']=='away'][['market_id','price']].rename(columns={'price':'price_away'})
    draw = odds_history[odds_history['side']=='draw'][['market_id','price']].rename(columns={'price':'price_draw'})

    match_winner = (
        home.rename(columns={'price':'price_home'})
        .merge(away, on='market_id', how='left')
        .merge(draw, on='market_id', how='left')
        [['market_id','price_home','price_draw','price_away','max_limit','pulled_at']]
    )

    # Calculate no-vig values
    match_winner[['overround','no_vig','home_no_vig','draw_no_vig','away_no_vig']] = \
        find_value_corrected_vectorized(match_winner)

    Ix2 = (
        match_winner
        .merge(markets[['market_id','event_id']], on='market_id')
        .merge(events, on='event_id')
        .sort_values(['event_id','pulled_at'])
    )

    first_last = Ix2.groupby('event_id').agg(
        first_home=('home_no_vig','first'),
        last_home=('home_no_vig','last'),
        first_away=('away_no_vig','first'),
        last_away=('away_no_vig','last')
    )

    first_last['home_total_move'] = first_last['last_home'] - first_last['first_home']
    first_last['away_total_move'] = first_last['last_away'] - first_last['first_away']

    # Filter events with net movement > 2%
    man_ml = first_last[
        (first_last['home_total_move'].abs() > 5) |
        (first_last['away_total_move'].abs() > 5)
    ].index


    subset = Ix2[Ix2['event_id'].isin(man_ml)]
    idx = subset.groupby('event_id')['pulled_at'].agg(['idxmin','idxmax'])
    first_last_rows = subset.loc[idx['idxmin'].tolist() + idx['idxmax'].tolist()]

    # Merge net movement totals
    result = first_last_rows.merge(
        first_last.loc[man_ml][['home_total_move','away_total_move']].reset_index(),
        on='event_id'
    )

    result = result.sort_values(['event_id','pulled_at'])

    final = (
        result.groupby('event_id')
        .agg({
            'league_name':'first',
            'home_team':'first',
            'away_team':'first',
            'starts':'first',
            'price_home':lambda x: f"{x.iloc[0]} -> {x.iloc[-1]}",
            'price_draw':lambda x: f"{x.iloc[0]} -> {x.iloc[-1]}",
            'price_away':lambda x: f"{x.iloc[0]} -> {x.iloc[-1]}",
            'home_total_move':'first',
            'away_total_move':'first',
            'pulled_at':'last'
        })
        .reset_index()
    )

    if final.empty:
        print('No vig movements detected')
    else:
        post_to_slack(final)
        return

check_odds(timedel=30)
