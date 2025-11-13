from .config import Config
from .gamma_client import GammaClient
from . import db_supabase as db
from .pipelines import minute as minute_pipeline
from datetime import datetime, timezone, timedelta
from .database.db_client import get_async_connection

UTC = timezone.utc

def _json_parse(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s and (s[0] in "[{" and s[-1] in "]}"):
            try:
                import json
                return json.loads(s)
            except Exception:
                return None
    return None

def _ts(x):
    """Normalize Gamma timestamps to tz-aware datetime (UTC).
    Accepts ISO strings, ms/seconds epoch, or datetime; returns datetime|None.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        # if milliseconds epoch
        if x > 1e12:
            x = x / 1000.0
        return datetime.fromtimestamp(float(x), tz=UTC)
    if isinstance(x, str):
        try:
            return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return None
    if isinstance(x, datetime):
        return x.astimezone(UTC) if x.tzinfo else x.replace(tzinfo=UTC)
    return None

def _as_array_of_numbers(x):
    arr = _json_parse(x)
    if arr is None:
        # If API returned nothing, use empty list (works with jsonb NOT NULL)
        return []
    if isinstance(arr, list):
        out = []
        for v in arr:
            try:
                out.append(float(v))
            except Exception:
                out.append(v)  # keep as-is if not numeric
        return out
    # scalar -> wrap
    try:
        return [float(arr)]
    except Exception:
        return [arr]

def _events_list(m):
    v = m.get("events")
    if isinstance(v, list):
        out = []
        for item in v:
            if isinstance(item, dict) and "id" in item:
                try:
                    out.append(int(item["id"]))
                except Exception:
                    continue
            else:
                try:
                    out.append(int(item))
                except Exception:
                    continue
        return out or None
    if m.get("eventId") is not None:
        try:
            return [int(m["eventId"])]
        except Exception:
            return None
    return None

def _markets_list(e):
    v = e.get("markets")
    if isinstance(v, list):
        try:
            return [int(x) for x in v]
        except Exception:
            return None
    if isinstance(v, (int, float)):
        return [int(v)]
    return None

async def run_once(cfg: Config, client: GammaClient, db_conn):
    try:
        markets = await client.fetch_markets()

        market_payload = []
        for m in markets:
            market_payload.append(
                {
                    "id": int(m.get("id") or m.get("marketId")),
                    "active": bool(m.get("active", True)),
                    "question": m.get("question"),
                    "description": m.get("description"),
                    "slug": m.get("slug"),
                    "image": m.get("image"),
                    "startDate": _ts(m.get("startDate")),
                    "endDate": _ts(m.get("endDate")),
                    "outcomes": _json_parse(m.get("outcomes")) or [],
                    "outcomePrices": _as_array_of_numbers(m.get("outcomePrices") or m.get("outcomePrice")),
                    "oneHourPriceChange": float(m.get("oneHourPriceChange", 0) or 0),
                    "oneDayPriceChange": float(m.get("oneDayPriceChange", 0) or 0),
                    "oneMonthPriceChange": float(m.get("oneMonthPriceChange", 0) or 0),
                    "oneWeekPriceChange": float(m.get("oneWeekPriceChange", 0) or 0),
                    "lastTradedPrice": float(
                        m.get("lastTradePrice", m.get("lastTradedPrice", 0)) or 0
                    ),
                    "bestAsk": float(m.get("bestAsk", 0) or 0),
                    "bestBid": float(m.get("bestBid", 0) or 0),
                    "volume": float(m.get("volume", m.get("volumeNum", 0)) or 0),
                    "volume24hr": float(m.get("volume24hr", 0) or 0),
                    "volume1wk": float(m.get("volume1wk", 0) or 0),
                    "volume1mo": float(m.get("volume1mo", 0) or 0),
                    "volume1yr": float(m.get("volume1yr", 0) or 0),
                    "liquidity": float(m.get("liquidity", m.get("liquidityNum", 0)) or 0),
                    "events": _events_list(m),
                }
            )

        await db.upsert_markets(db_conn, market_payload)

        events = await client.fetch_events()

        event_payload = []
        for e in events:
            event_payload.append({
                "id": int(e.get("id")),
                "active": bool(e.get("active", True)),
                "title": e.get("title"),
                "slug": e.get("slug"),
                "description": e.get("description"),
                "tags": _json_parse(e.get("tags")),
                "image": e.get("image"),
                "startDate": _ts(e.get("startDate")),
                "endDate": _ts(e.get("endDate")),
                "competitive": float(e.get("competitive", 0) or 0),
                "liquidity": float(e.get("liquidity", e.get("liquidityNum", 0)) or 0),
                "volume": float(e.get("volume", e.get("volumeNum", 0)) or 0),
                "volume24hr": float(e.get("volume24hr", 0) or 0),
                "volume1wk": float(e.get("volume1wk", 0) or 0),
                "volume1mo": float(e.get("volume1mo", 0) or 0),
                "volume1yr": float(e.get("volume1yr", 0) or 0),
                "markets": _markets_list(e),
            })

        await db.upsert_events(db_conn, event_payload)

        rows = await minute_pipeline.write_minute(db_conn, markets, datetime.now(tz=UTC))

        now = datetime.now(tz=UTC)

        if now.minute == 0 and rows:
            await db.upsert_hours(db_conn, rows)
        if now.hour == 0 and now.minute == 0 and rows:
            await db.upsert_days(db_conn, rows)
    except Exception as e:
        print(f"Error during run_once: {e}")


async def seconds_until_next_minute(now=None) -> float:
    now = now or datetime.now(tz=UTC)
    nxt = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.5)

async def loop(cfg: Config, client: GammaClient):
    db_conn = None
    try:

        db_conn = await get_async_connection()
        print("Database connection established.")

        while True:
            await run_once(cfg, client, db_conn)

            # archive + prune hot data (optional, enable when ready)
            # await archive_and_prune_minutes(sb, table=db.TBL_M_1M,
            # older_than=now - timedelta(minutes=cfg.retention_minute_minutes),
            # bucket=cfg.s3_bucket, region=cfg.s3_region, prefix=cfg.s3_prefix)
            # await archive_and_prune_minutes(sb, table=db.TBL_M_1H,
            # older_than=now - timedelta(days=cfg.retention_hour_days),
            # bucket=cfg.s3_bucket, region=cfg.s3_region, prefix=cfg.s3_prefix)

            await asyncio.sleep(await seconds_until_next_minute())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutdown requested (Ctrl+C or Cancelled)...")
    finally:
        print("Cleaning up connections...")
        if db_conn:
            await db_conn.close()
            print("Database connection closed.")
        
        await client.close()
        print("GammaClient closed. Exiting.")