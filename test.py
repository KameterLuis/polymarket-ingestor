from ingestor.gamma_client import GammaClient
import asyncio
from ingestor.config import Config
from ingestor.scheduler import run_once
from ingestor import db_supabase as db
import time
from datetime import datetime, timezone, timedelta
from ingestor.database.db_client import get_async_connection

UTC = timezone.utc

async def seconds_until_next_minute(now=None) -> float:
    now = now or datetime.now(tz=UTC)
    nxt = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.5)

async def main():
    cfg = Config.from_env()
    client = GammaClient()
    db_conn = None
    try:
        db_conn = await get_async_connection()
        print("Database connection established.")

        for i in range(0, 4):
            now = time.time()
            await run_once(cfg, client, db_conn)
            print("Run took ", time.time() - now)
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

    #cfg = Config.from_env()
    #client = GammaClient()
    #start = time.time()
    #await run_once(cfg, client)
    #print("Took", time.time() - start)
    #await client.close()

if __name__ == "__main__":
    asyncio.run(main())
