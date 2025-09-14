import os
import time
import httpx
import json
import redis.asyncio as redis
import logging
import sys
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno <= logging.INFO

# Create custom handlers
info_handler = logging.StreamHandler(sys.stdout)
info_handler.setLevel(logging.INFO)
info_handler.addFilter(InfoFilter())

error_handler = logging.StreamHandler(sys.stderr)
error_handler.setLevel(logging.WARNING)

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(info_handler)
logger.addHandler(error_handler)

# Prevent duplicate logs
logger.propagate = False

httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.INFO)
httpx_logger.addHandler(info_handler)
httpx_logger.addHandler(error_handler)
httpx_logger.propagate = False

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
    return "This is the London Student Network polling and scheduling service..."

@app.get("/api/health")
async def confirm_healthy(request: Request):
    """A simple, secured health check for this service."""
    expected_auth_header = f"Bearer {CRON_SECRET}"
    if not CRON_SECRET:
        raise HTTPException(status_code=500, detail="CRON_SECRET environment variable not set")
    if request.headers.get("Authorization") != expected_auth_header:
        raise HTTPException(status_code=401, detail="Unauthorized")

    redis_client = await get_redis_client()
    try:
        await redis_client.ping()
        return {"status": "success", "message": "Service is healthy and Redis connection is OK."}
    finally:
        await redis_client.close()

@app.get("/api/cron/poll-instagram")
async def poll_instagram_and_enqueue(request: Request):
    """Polls Instagram for new posts and pushes them to a Redis list."""
    expected_auth_header = f"Bearer {CRON_SECRET}"
    if not CRON_SECRET:
        raise HTTPException(status_code=500, detail="CRON_SECRET environment variable not set")
    if request.headers.get("Authorization") != expected_auth_header:
        raise HTTPException(status_code=401, detail="Unauthorized")

    redis_client = await get_redis_client()
    
    try:
        logger.info("Starting Instagram polling job")
        active_user_ids = await redis_client.smembers("instagram_polling_list")
        new_posts_found = 0
        errors = []

        if not active_user_ids:
            logger.info("No active users to poll")
            return JSONResponse(status_code=200, content={"status": "complete", "message": "No active users to poll."})

        logger.info(f"Polling {len(active_user_ids)} users")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for user_id in active_user_ids:
                try:
                    logger.info(f"Processing user {user_id}")
                    user_cache_key = f"user:{user_id}:instagram"
                    cached_data = await redis_client.hgetall(user_cache_key)
                    access_token = cached_data.get("access_token")

                    poll_start_time = int(time.time())
                    last_polled_timestamp = cached_data.get("last_polled_timestamp", str(poll_start_time - 3600))  # Default to 1 hour ago
                    
                    if not access_token:
                        error_msg = f"No access token for user {user_id}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
                        continue

                    current_api_url = f"https://graph.instagram.com/me/media?fields=id,caption,media_type,media_url,permalink,timestamp&since={last_polled_timestamp}&access_token={access_token}"

                    while current_api_url:
                        try:
                            logger.info(f"Making API request for user {user_id}")
                            response = await client.get(current_api_url)
                            response.raise_for_status()
                            json_data = response.json()
                            
                            if "error" in json_data:
                                error_msg = f"Instagram API error for user {user_id}: {json_data['error']}"
                                logger.error(error_msg)
                                errors.append(error_msg)
                                break
                            
                            posts = json_data.get("data", [])
                            logger.info(f"Found {len(posts)} posts for user {user_id}")

                            for post in posts:
                                if post.get("media_type") == "IMAGE":
                                    job_data = { 
                                        "user_id": user_id,
                                        "post_id": post.get("id"), 
                                        "post_url": post.get("permalink"),
                                        "media_url": post.get("media_url"),
                                        "caption": post.get("caption", "") 
                                    }
                                    await redis_client.lpush("instagram_jobs_queue", json.dumps(job_data))
                                    new_posts_found += 1
                                    logger.info(f"Queued new post {post.get('id')} for user {user_id}")
                            
                            current_api_url = json_data.get("paging", {}).get("next")
                            
                        except httpx.HTTPStatusError as e:
                            error_msg = f"HTTP error for user {user_id}: {e.response.status_code} - {e.response.text}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                            break
                        except Exception as e:
                            error_msg = f"Unexpected error processing user {user_id}: {str(e)}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                            break
                    
                    await redis_client.hset(user_cache_key, "last_polled_timestamp", str(poll_start_time))
                    logger.info(f"Updated last_polled_timestamp for user {user_id}")
                    
                except Exception as e:
                    error_msg = f"Critical error processing user {user_id}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

        response_data = {
            "status": "success" if not errors else "partial_success",
            "message": f"Found {new_posts_found} new posts",
            "new_posts_found": new_posts_found,
            "users_processed": len(active_user_ids),
            "errors": errors if errors else None
        }
        
        logger.info(f"Polling job completed: {response_data}")
        return JSONResponse(status_code=200, content=response_data)
    
    except Exception as e:
        error_msg = f"Critical error in polling job: {str(e)}"
        logger.error(error_msg)
        return JSONResponse(status_code=500, content={"status": "error", "message": error_msg})
    
    finally:
        await redis_client.close()
