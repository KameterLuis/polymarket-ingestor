import asyncio
import os
import json
from datetime import datetime, timezone, timedelta
from .gamma_client import GammaClient
from .helpers import _json_parse, _ts, _as_array_of_numbers, _events_list, _markets_list, seconds_until_next_minute, _events_to_redis_pipeline
import redis
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

AWS_SQS_QUEUE_URL = os.getenv("AWS_SQS_QUEUE_URL")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_S3_REGION", "us-east-1")

try:
    redis_client = redis.from_url(
        UPSTASH_URL,
        password=UPSTASH_TOKEN,
        decode_responses=True
    )
    redis_client.ping()
    print("✅ Successfully connected to Upstash (Redis)")
except Exception as e:
    print(f"❌ ERROR connecting to Upstash (Redis): {e}")
    redis_client = None

try:
    sqs_client = boto3.client(
        'sqs',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    print("✅ Successfully configured AWS SQS client")
except Exception as e:
    print(f"❌ ERROR configuring SQS client: {e}")
    sqs_client = None

MARKET_CACHE_TTL_SECONDS = 60 * 60 
SQS_BATCH_SIZE = 200 

def _markets_to_redis_pipeline(pipe, markets: list[dict]):
    print(f"  > Preparing {len(markets)} markets for Redis cache...")
    for market in markets:
        try:
            key = f"market:{market['id']}"
            value = json.dumps(market)
            pipe.set(key, value, ex=MARKET_CACHE_TTL_SECONDS)
        except Exception as e:
            print(f"  > Error preparing market {market.get('id')} for Redis: {e}")
    return pipe

def _send_sqs_batch(batch: list[dict]):
    try:
        message_body = json.dumps(batch)
        
        response = sqs_client.send_message(
            QueueUrl=AWS_SQS_QUEUE_URL,
            MessageBody=message_body
        )
        return response['MessageId']
    except ClientError as e:
        print(f"  > ❌ SQS Send Error: {e}")
        return None

async def run_once(client: GammaClient):
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

        print(f"Fetched {len(market_payload)} markets and {len(event_payload)} events.")

        if redis_client:
            try:
                # Use a pipeline for blazing-fast bulk uploads
                pipe = redis_client.pipeline()
                
                # Add all markets to the pipeline
                _markets_to_redis_pipeline(pipe, market_payload)
                
                # Add all events to the pipeline (using "event:123" key)
                # (You'll need a new helper `_events_to_redis_pipeline`)
                _events_to_redis_pipeline(pipe, event_payload)
                
                print("  > Sending bulk upload to Upstash (Redis)...")
                await asyncio.to_thread(pipe.execute)
                print("  > ✅ Upstash cache updated.")
                
            except Exception as e:
                print(f"  > ❌ Upstash (Redis) Error: {e}")
        
        if sqs_client:
            print(f"  > Preparing {len(market_payload)} markets for SQS queue...")
            batch_count = 0
            for i in range(0, len(market_payload), SQS_BATCH_SIZE):
                batch = market_payload[i:i + SQS_BATCH_SIZE]
                message_id = await asyncio.to_thread(_send_sqs_batch, batch)
                
                if message_id:
                    batch_count += 1
                
            print(f"  > ✅ SQS queue updated with {batch_count} batches.")

    except Exception as e:
        print(f"Error during run_once: {e}")

async def loop(client: GammaClient):
    try:
        if not redis_client or not sqs_client:
            print("❌ Critical component not initialized (Redis or SQS). Exiting.")
            return

        while True:
            await run_once(client)
            await asyncio.sleep(await seconds_until_next_minute())
            
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutdown requested (Ctrl+C or Cancelled)...")
        
    finally:
        print("Cleaning up connections...")
        if redis_client:
            redis_client.close()
            print("Upstash (Redis) connection closed.")
        
        await client.close()
        print("GammaClient closed. Exiting.")