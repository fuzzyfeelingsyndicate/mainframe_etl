import os
import requests
from supabase import create_client, Client
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def cet_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))

MAX_EVENT_AGE = timedelta(days=7)

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
rapid_url = os.getenv("RAPID_URL_MARKETS")
rapid_api_key = os.getenv("RAPID_API_KEY")
rapid_api_host = os.getenv("RAPID_API_HOST")


def upsert_event(events, event_created: dict):
    """
    Upsert events into the database.
    Uses a single upsert call instead of separate insert/update per row.
    Populates event_created with the canonical created_at for each event.
    """
    all_ids = [e["event_id"] for e in events["events"]]

    existing_rows = (
        supabase.table("events")
        .select("event_id", "created_at")
        .in_("event_id", all_ids)
        .execute()
    ).data
    existing_map = {r["event_id"]: r["created_at"] for r in existing_rows}

    now_cet = cet_now()
    rows_to_upsert = []

    for event in events["events"]:
        event_id = event["event_id"]
        is_new = event_id not in existing_map

        # Use the original created_at for existing events to preserve it
        created_at = now_cet.isoformat() if is_new else existing_map[event_id]

        rows_to_upsert.append({
            "event_id": event_id,
            "sport_id": event["sport_id"],
            "league_id": event["league_id"],
            "league_name": event["league_name"],
            "starts": event["starts"],
            "home_team": event["home"],
            "away_team": event["away"],
            "created_at": created_at,
        })

        # Populate in-memory created_at map
        event_created[event_id] = (
            now_cet if is_new
            else datetime.fromisoformat(existing_map[event_id])
        )

        if is_new:
            post_to_slack(
                f":sparkles: New event added: *{event['home']}* vs *{event['away']}*"
                f" in *{event['league_name']}* — starts {event['starts']}"
            )

    if rows_to_upsert:
        supabase.table("events").upsert(
            rows_to_upsert, on_conflict="event_id"
        ).execute()


def load_markets(event_ids: list, market_cache: dict):
    """Load existing markets from DB into market_cache for the given event_ids."""
    if not event_ids:
        return
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


def load_latest_odds(market_ids: list) -> dict:
    """Return the most recent price per (market_id, side) within the last hour."""
    if not market_ids:
        return {}
    cutoff = (cet_now() - timedelta(hours=1)).isoformat()
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


def upsert_market(event, period, event_created: dict, market_cache: dict):
    """
    Upsert a market row and return its market_id.
    Returns None if the event is unknown or too old.
    """
    event_id = event["event_id"]
    now = cet_now()  # single call to avoid drift

    if event_id not in event_created:
        return None
    if now - event_created[event_id] > MAX_EVENT_AGE:
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
            "created_at": now.isoformat(),
        }
        result = (
            supabase.table("markets")
            .upsert(data, on_conflict="event_id,line_id,market_type,parameter")
            .execute()
        )
        market_id = result.data[0]["market_id"]
        market_cache[cache_key] = market_id

    return market_cache[cache_key]


def insert_odds(event_id, market_id, side, price, max_limit, latest_odds: dict, event_created: dict):
    """Insert an odds record only if the price has changed since the last pull."""
    now = cet_now()  # single call to avoid drift

    if event_id not in event_created:
        return
    if now - event_created[event_id] > MAX_EVENT_AGE:
        return

    # Guard against non-numeric prices (e.g. API wraps value in an object)
    if not isinstance(price, (int, float)):
        print(f"Unexpected price type for market_id={market_id} side={side}: {price!r}")
        return

    if latest_odds.get((market_id, side)) == price:
        return

    supabase.table("odds_history").insert({
        "event_id": event_id,
        "market_id": market_id,
        "side": side,
        "price": price,
        "max_limit": max_limit,
        "pulled_at": now.isoformat(),
    }).execute()


def process_event(events):
    """
    Main processing pipeline for one API response batch.
    State (event_created, market_cache) is local to this call so batches
    across different sports don't bleed into each other.
    """
    # Isolate state per batch to avoid cross-sport contamination
    event_created: dict = {}
    market_cache: dict = {}

    upsert_event(events, event_created)

    # Only process events we have a created_at for (i.e. not silently dropped)
    valid_ids = [
        e["event_id"] for e in events["events"]
        if e["event_id"] in event_created
    ]

    load_markets(valid_ids, market_cache)

    # Load latest odds only for markets relevant to this batch
    market_ids = list(market_cache.values())
    latest_odds = load_latest_odds(market_ids)

    for event in events["events"]:
        event_id = event["event_id"]
        for _key, period in event.get("periods", {}).items():
            if period.get("money_line") and period.get("number") == 0:
                market_id = upsert_market(event, period, event_created, market_cache)
                if not market_id:
                    continue
                for side, price in period["money_line"].items():
                    insert_odds(
                        event_id, market_id, side, price,
                        period.get("max_limit", 0),
                        latest_odds, event_created
                    )


def get_active_leagues() -> dict:
    """Fetch active leagues from Supabase, grouped by sport_id."""
    result = (
        supabase.table("leagues")
        .select("league_id, sport_id")
        .eq("is_active", True)
        .execute()
    )

    if not result.data:
        post_to_slack(":warning: No active leagues found in database!")
        return {}

    leagues_by_sport: dict = {}
    for row in result.data:
        sport_id = row["sport_id"]
        leagues_by_sport.setdefault(sport_id, []).append(str(row["league_id"]))

    return leagues_by_sport


if __name__ == "__main__":
    api_url = rapid_url
    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host,
    }

    leagues_by_sport = get_active_leagues()

    if not leagues_by_sport:
        print("No active leagues configured. Exiting.")
        raise SystemExit(0)

    for sport_id, league_ids in leagues_by_sport.items():
        league_ids_str = ",".join(league_ids)
        querystring = {
            "league_ids": league_ids_str,
            "event_type": "prematch",
            "sport_id": sport_id,
            "is_have_odds": "true",
        }

        print(f"Fetching odds for sport_id={sport_id}, leagues: {league_ids_str}")

        try:
            response = requests.get(api_url, headers=headers, params=querystring, timeout=30)
            response.raise_for_status()
            event_list = response.json()
            process_event(event_list)
        except requests.RequestException as e:
            print(f"API request failed for sport_id={sport_id}: {e}")
            post_to_slack(f":x: Failed to fetch odds for sport_id={sport_id}: {e}")
            continue
