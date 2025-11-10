# smoke_test.py
from dotenv import load_dotenv

load_dotenv()

import asyncio
from datetime import datetime, timezone

from aiolimiter import AsyncLimiter

from polymarket_ingestor import db_supabase as db
from polymarket_ingestor.config import Config
from polymarket_ingestor.gamma_client import GammaClient
from polymarket_ingestor.scheduler import run_once

UTC = timezone.utc


async def main():
    cfg = Config.from_env()
    limiter = AsyncLimiter(cfg.reqs_per_window, cfg.window_seconds)
    client = GammaClient(cfg.gamma_base, limiter)
    sb = db.connect(cfg.supabase_url, cfg.supabase_key)

    # run a single ingestion pass
    await run_once(cfg, sb, client)
    print("Ingestion OK")

    # quick counts
    m = sb.table("markets").select("*", count="exact").limit(1).execute()
    e = sb.table("events").select("*", count="exact").limit(1).execute()
    c = sb.table("market_candles_1m").select("*", count="exact").limit(1).execute()
    print("markets:", m.count, "events:", e.count, "minute candles:", c.count)

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
