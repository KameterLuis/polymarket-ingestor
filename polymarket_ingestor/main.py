import asyncio

from dotenv import load_dotenv

from .config import Config
from .scheduler import loop

load_dotenv()

if __name__ == "__main__":
    cfg = Config.from_env()
    try:
        asyncio.run(loop(cfg))
    except KeyboardInterrupt:
        pass
