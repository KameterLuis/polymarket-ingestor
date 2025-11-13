from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from .database.db_client import replace_rows, upsert_rows

UTC = timezone.utc

# Table names
TBL_M_1M = "market_candles_1m"
TBL_M_1H = "market_candles_1h"
TBL_M_1D = "market_candles_1d"
TBL_MARKETS = "markets"
TBL_EVENTS = "events"

UPSERT_CHUNK = 1500

EVENTS_COLS  = ["id", "active", "title", "slug", "description", "image", "startDate", "endDate", "competitive", "liquidity", "volume", "volume24hr", "volume1wk", "volume1mo", "volume1yr", "markets", "tags"]
MARKETS_COLS = ["id", "active", "question", "description", "slug", "image", "startDate", "endDate", "oneHourPriceChange", "oneDayPriceChange", "oneMonthPriceChange", "oneWeekPriceChange", "lastTradedPrice", "bestAsk", "bestBid", "volume", "volume24hr", "volume1wk", "volume1mo", "volume1yr", "liquidity", "events", "outcomes", "outcomePrices"]

C1M_COLS = ["market_id", "volume", "price", "ts"]
C1H_COLS = C1M_COLS
C1D_COLS = C1M_COLS

KEY = ("market_id", "ts")

async def upsert_markets(conn, payload: list[dict]):
    """Upserts all markets. Accepts the psycopg connection."""
    print("upserting markets")
    now = time.time()
    
    json_cols = {"outcomes", "outcomePrices", "events"} 
    key_cols = ["id"]
    
    volatile_cols = [
        "active",
        "oneHourPriceChange",
        "oneDayPriceChange",
        "oneMonthPriceChange",
        "oneWeekPriceChange",
        "lastTradedPrice",
        "bestAsk",
        "bestBid",
        "volume",
        "volume24hr",
        "volume1wk",
        "volume1mo",
        "volume1yr",
        "liquidity",
        "outcomePrices"
    ]
    
    await upsert_rows(
        conn, 
        "public", 
        TBL_MARKETS, 
        MARKETS_COLS, 
        key_cols,
        payload, 
        json_cols=json_cols,
        volatile_cols=volatile_cols  # <-- Pass the list here
    )
    # --------------------------
    
    print(f"Done upserting markets. Took {time.time() - now:.2f}s")

async def upsert_events(conn, payload: list[dict]):
    """Upserts all events. Accepts the psycopg connection."""
    print("upserting events")
    
    json_cols = {"tags", "markets"}
    key_cols = ["id"]  # Assuming 'id' is the primary key for events

    # --- THIS IS THE NEW LOGIC ---

    # Define the columns that change frequently.
    # Static columns like title, slug, description, image, startDate, 
    # and endDate will be left alone during updates.
    #
    # !! Review this list !!
    volatile_cols = [
        "active",
        "competitive",
        "liquidity",
        "volume",
        "volume24hr",
        "volume1wk",
        "volume1mo",
        "volume1yr",
    ]

    await upsert_rows(
        conn,
        "public",
        TBL_EVENTS,
        EVENTS_COLS,
        key_cols,
        payload,
        json_cols=json_cols,
        volatile_cols=volatile_cols
    )
    
    print("done upserting events")

async def upsert_minutes(conn, rows):
    """Upserts minute-by-minute data. Accepts psycopg connection."""
    print("upserting minutes")
    
    await upsert_rows(conn, "public", TBL_M_1M, C1M_COLS, KEY, rows)
    
    print("done upserting minutes")

async def upsert_hours(conn, rows: Iterable[tuple] | List[Dict]):
    """Upserts hourly data. Accepts psycopg connection."""
    await upsert_rows(conn, "public", TBL_M_1H, C1H_COLS, KEY, rows)

async def upsert_days(conn, rows: Iterable[tuple] | List[Dict]):
    """Upserts daily data. Accepts psycopg connection."""
    await upsert_rows(conn, "public", TBL_M_1D, C1D_COLS, KEY, rows)