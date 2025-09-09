import os
import time
import httpx
import json
import redis.asyncio as redis
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Environment Variable Setup ---
REDIS_URL = os.getenv("REDIS_URL")
CRON_SECRET = os.getenv("CRON_SECRET")

async def get_redis_client():
    """Create a new Redis client for each request (serverless-friendly)"""
    if not REDIS_URL:
        raise HTTPException(status_code=500, detail="REDIS_URL environment variable not set")
    
    try:
        client = redis.from_url(REDIS_URL, decode_responses=True)
        # Test the connection
        await client.ping()
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis connection failed: {str(e)}")

# --- Initialize App ---
app = FastAPI(
    title="London Student Network Polling & Scheduling Service"
)

# --- CORS Middleware ---
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- API Routes ---
@app.get("/")
async def welcome():
    return "This is the protected London Student Network polling and scheduling service..."

@app.get("/api/health")
async def confirm_healthy(request: Request):
    """A simple, secured health check for this service."""
    # expected_auth_header = f"Bearer {CRON_SECRET}"
    # if not CRON_SECRET or request.headers.get("Authorization") != expected_auth_header:
    #     raise HTTPException(status_code=401, detail="Unauthorized")

    redis_client = await get_redis_client()
    try:
        await redis_client.ping()
        return {"status": "success", "message": "Service is healthy and Redis connection is OK."}
    finally:
        await redis_client.close()

@app.get("/api/cron/poll-instagram")
async def poll_instagram_and_enqueue(request: Request):
    """Polls Instagram for new posts and pushes them to a Redis list."""
    # expected_auth_header = f"Bearer {CRON_SECRET}"
    # if not CRON_SECRET or request.headers.get("Authorization") != expected_auth_header:
    #     raise HTTPException(status_code=401, detail="Unauthorized")

    redis_client = await get_redis_client()
    
    try:
        active_user_ids = await redis_client.smembers("instagram_polling_list")
        new_posts_found = 0

        if not active_user_ids:
            return JSONResponse(status_code=200, content={"status": "complete", "message": "No active users to poll."})

        async with httpx.AsyncClient() as client:
            for user_id in active_user_ids:
                user_cache_key = f"user:{user_id}:instagram"
                cached_data = await redis_client.hgetall(user_cache_key)
                access_token = cached_data.get("access_token")

                poll_start_time = int(time.time())
                # last_polled_timestamp = cached_data.get("last_polled_timestamp", poll_start_time)
                last_polled_timestamp = 0          
                if not access_token:
                    print(f"Warning: No access token for user {user_id}. Skipping.")
                    continue

                current_api_url = f"https://graph.instagram.com/me/media?fields=id,caption,media_type,permalink,timestamp&since={last_polled_timestamp}"

                while current_api_url:
                    try:
                        response = await client.get(current_api_url, headers={"Authorization": f"Bearer {access_token}"})
                        response.raise_for_status()
                        json_data = response.json()
                        posts = json_data.get("data", [])

                        for post in posts:
                            if post.get("media_type") == "IMAGE":
                                job_data = { "post_id": post.get("id"), "post_url": post.get("permalink"), "caption": post.get("caption", "") }
                                # Push a JSON string to a Redis list named 'instagram_jobs_queue'
                                await redis_client.lpush("instagram_jobs_queue", json.dumps(job_data))
                                new_posts_found += 1
                        
                        current_api_url = json_data.get("paging", {}).get("next")
                    except Exception as e:
                        print(f"Error processing user {user_id}: {e}")
                        current_api_url = None
                
                await redis_client.hset(user_cache_key, "last_polled_timestamp", poll_start_time)

        return JSONResponse(status_code=200, content={"status": "success", "message": f"number of new posts found: ${new_posts_found}"})
    
    finally:
        await redis_client.close()
