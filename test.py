# test.py
import asyncio
from ingestor.scheduler import loop
from ingestor.gamma_client import GammaClient

async def main():
    print("Starting ingestor test...")
    
    client = GammaClient() 
    
    try:
        # 3. Run the main loop
        await loop(client)
    except KeyboardInterrupt:
        print("Test stopped manually.")
    finally:
        # The loop's finally block will handle closing the client
        print("Test finished.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass