from ingestor.gamma_client import GammaClient
import asyncio
from ingestor.config import Config
from ingestor.scheduler import run_once
from ingestor import db_supabase as db
import time

async def main():
    start = time.time()
    cfg = Config.from_env()
    client = GammaClient()
    sb = db.connect(cfg.supabase_url, cfg.supabase_key)
    await run_once(cfg, sb, client)
    await client.close()
    print("Took", time.time() - start)

if __name__ == "__main__":
    asyncio.run(main())
