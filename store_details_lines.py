import pandas as pd
import numpy as np
from typing import Union


# dtype mapping for minimal memory footprint in parquet
DTYPES = {
    "event_id": "int64",
    "sport_id": "int16",
    "league_id": "int32",
    "league_name": "category",
    "home_team": "category",
    "away_team": "category",
    "starts": "str",
    "period": "int8",
    "market_type": "category",
    "line": "float32",
    "side": "category",
    "timestamp": "int64",
    "odds": "float64",
    "max_limit": "int32",
}


def _extract_moneyline(history: dict, meta: dict) -> list[dict]:
    """Extract moneyline history entries (home, draw, away)."""
    rows = []
    for side, entries in history.items():
        for ts, odds, limit in entries:
            rows.append({
                "market_type": "moneyline",
                "line": 0.0,
                "side": side,
                "timestamp": ts,
                "odds": odds,
                "max_limit": limit,
            })
    return rows


def _extract_spreads(history: dict, meta: dict) -> list[dict]:
    """Extract spread history entries across all lines."""
    rows = []
    for line_val, sides in history.items():
        line_f = float(line_val)
        for side, entries in sides.items():
            for ts, odds, limit in entries:
                rows.append({
                    "market_type": "spread",
                    "line": line_f,
                    "side": side,
                    "timestamp": ts,
                    "odds": odds,
                    "max_limit": limit,
                })
    return rows


def _extract_totals(history: dict, meta: dict) -> list[dict]:
    """Extract totals history entries across all lines."""
    rows = []
    for line_val, sides in history.items():
        line_f = float(line_val)
        for side, entries in sides.items():
            for ts, odds, limit in entries:
                rows.append({
                    "market_type": "total",
                    "line": line_f,
                    "side": side,
                    "timestamp": ts,
                    "odds": odds,
                    "max_limit": limit,
                })
    return rows


def extract_period0_history(api_response: dict) -> pd.DataFrame:
    """
    Extract moneyline, spread, and total history from period num_0
    for all events in the API response.

    Args:
        api_response: Raw JSON response with 'events' key.

    Returns:
        DataFrame with columns:
            event_id, sport_id, league_id, league_name, home_team, away_team,
            starts, period, market_type, line, side, timestamp, odds, max_limit
    """
    all_rows = []

    for event in api_response.get("events", []):
        event_id = event["event_id"]
        period_data = event.get("periods", {}).get("num_0")
        if not period_data:
            continue

        history = period_data.get("history", {})
        meta = period_data.get("meta", {})

        event_meta = {
            "event_id": event_id,
            "sport_id": event.get("sport_id"),
            "league_id": event.get("league_id"),
            "league_name": event.get("league_name"),
            "home_team": event.get("home"),
            "away_team": event.get("away"),
            "starts": event.get("starts"),
            "period": 0,
        }

        extractors = {
            "moneyline": _extract_moneyline,
            "spreads": _extract_spreads,
            "totals": _extract_totals,
        }

        for key, extractor in extractors.items():
            market_history = history.get(key, {})
            if not market_history:
                continue
            market_rows = extractor(market_history, meta)
            for row in market_rows:
                row.update(event_meta)
            all_rows.extend(market_rows)

    if not all_rows:
        return pd.DataFrame(columns=list(DTYPES.keys()))

    df = pd.DataFrame(all_rows)
    for col, dtype in DTYPES.items():
        if col in df.columns:
            if dtype == "category":
                df[col] = df[col].astype("category")
            else:
                df[col] = df[col].astype(dtype)

    col_order = [c for c in DTYPES if c in df.columns]
    return df[col_order]


def save_to_parquet(df: pd.DataFrame, path: str) -> None:
    """Save DataFrame to parquet with optimal compression."""
    df.to_parquet(
        path,
        engine="pyarrow",
        compression="zstd",
        index=False,
        use_dictionary=["league_name", "home_team", "away_team", "market_type", "side"],
    )
