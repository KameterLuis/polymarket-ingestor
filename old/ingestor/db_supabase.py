from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List
from .database.db_client import replace_rows, upsert_rows
import aiobotocore
import io
import os

UTC = timezone.utc

async def archive_and_delete_minutes(conn, table_name: str, retention_period: timedelta):
    """
    1. Selects all rows from `table_name` older than the `retention_period`.
    2. Streams them as a CSV to S3.
    3. If the upload is successful, deletes the rows from Postgres.
    """
    print(f"Starting archive for {table_name}...")
    
    # 1. Get S3 settings from environment
    bucket_name = os.getenv("AWS_S3_BUCKET")
    region = os.getenv("AWS_S3_REGION")
    if not bucket_name:
        print("Error: AWS_S3_BUCKET not set. Skipping archive.")
        return

    # 2. Define queries and S3 key
    cutoff_ts = datetime.now(timezone.utc) - retention_period
    
    # We will name the S3 file based on the time it was archived
    now_ts = datetime.now(timezone.utc)
    s3_key = f"{table_name}/{now_ts.year}/{now_ts.month:02d}/{now_ts.day:02d}/{now_ts.isoformat()}.csv"

    # Use SQL for safe table/column names
    tbl = sql.Identifier(table_name)
    select_query = sql.SQL("SELECT * FROM {tbl} WHERE ts < %s FOR UPDATE").format(tbl=tbl)
    delete_query = sql.SQL("DELETE FROM {tbl} WHERE ts < %s").format(tbl=tbl)
    
    # This magic query tells Postgres to create a CSV from our SELECT
    copy_query = sql.SQL("COPY ({select_query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)").format(
        select_query=select_query
    )

    # 3. Create S3 client
    session = aiobotocore.get_session()
    async with session.create_client(
        's3',
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    ) as s3_client:
        
        # 4. Start transaction
        await conn.set_autocommit(False)
        try:
            async with conn.cursor() as cur:
                
                # 5. Stream data from Postgres using COPY
                print(f"  Fetching data older than {cutoff_ts}...")
                buf = io.BytesIO()
                async with cur.copy(copy_query, (cutoff_ts,)) as copy:
                    async for data in copy:
                        buf.write(data)
                
                buf.seek(0)
                csv_data = buf.read()
                
                if not csv_data:
                    print("  No data to archive.")
                    await conn.commit()  # Commit to release the (empty) transaction
                    return

                print(f"  Uploading {len(csv_data)} bytes to s3://{bucket_name}/{s3_key}")
                
                # 6. Upload data to S3
                await s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=csv_data
                )
                
                # 7. SUCCESS! Now delete the data
                print("  Upload successful. Deleting old rows from Postgres...")
                await cur.execute(delete_query, (cutoff_ts,))
                
                await conn.commit()
                print("  Archive and delete complete.")

        except Exception as e:
            print(f"Archive FAILED: {e}")
            await conn.rollback()
            # Re-raise the exception so the scheduler can see it
            raise e

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