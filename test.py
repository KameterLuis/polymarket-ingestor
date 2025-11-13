from ingestor.gamma_client import GammaClient
import asyncio
from ingestor.config import Config
from ingestor.scheduler import run_once
from ingestor import db_supabase as db
import time

async def main():
    cfg = Config.from_env()
    client = GammaClient()
    sb = db.connect(cfg.supabase_url, cfg.supabase_key)
    start = time.time()
    await run_once(cfg, sb, client)
    print("Took", time.time() - start)
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
