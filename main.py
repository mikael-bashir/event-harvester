import asyncio
import os
import redis.asyncio as redis

# --- Configuration ---
# Make sure your REDIS_URL is available as an environment variable
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

async def main():
    """
    A simple async function to connect to Redis and test basic operations.
    """
    print("--- Starting Redis Asyncio Test ---")

    # Explicitly type hint the client to help Pylance understand it's an async client
    redis_client: redis.Redis = redis.from_url(REDIS_URL, decode_responses=True)

    try:
        # 1. Test connection with ping()
        # This is a coroutine and MUST be awaited.
        ping_response = await redis_client.ping()
        print(f"1. Ping successful: {ping_response}")

        # 2. Test SET and GET
        await redis_client.set("pylance_test_key", "hello_async")
        value = await redis_client.get("pylance_test_key")
        print(f"2. SET 'pylance_test_key', then GET: '{value}'")

        # 3. Test LPUSH and LREM (the problematic commands)
        list_key = "pylance_test_list"
        await redis_client.lpush(list_key, "item1", "item2", "item1")
        print("3. LPUSHED three items to 'pylance_test_list'")
        
        # lrem is a coroutine and MUST be awaited. It returns an integer.
        removed_count = await redis_client.lrem(list_key, 1, "item1")
        print(f"   LREM removed {removed_count} instance(s) of 'item1'")
        
        # 4. Test SADD and SMEMBERS
        set_key = "pylance_test_set"
        await redis_client.sadd(set_key, "user:1", "user:2", "user:3")
        members = await redis_client.smembers(set_key)
        print(f"4. SADD users, then SMEMBERS: {members}")

    except Exception as e:
        print(f"\n--- An error occurred ---")
        print(e)
    finally:
        # --- Cleanup ---
        print("\n--- Cleaning up test keys ---")
        await redis_client.delete("pylance_test_key", "pylance_test_list", "pylance_test_set")
        # Always close the connection pool when you're done
        await redis_client.aclose()
        print("--- Test finished and connection closed ---")


if __name__ == "__main__":
    # This runs the main async function
    asyncio.run(main())


'''
### How to Run This Test

1.  **Save the Code:** Save the content of the Canvas as `redis_test.py`.
2.  **Ensure Redis is Running:** Make sure you have a Redis server accessible at the `REDIS_URL` you've configured.
3.  **Run from Terminal:** Execute the script directly from your terminal.

'''
