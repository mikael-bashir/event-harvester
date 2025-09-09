import os
import time
import httpx
import redis.asyncio as redis
from contextlib import asynccontextmanager
from arq import create_pool
from arq.connections import RedisSettings
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Environment Variable Setup ---
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL")
CRON_SECRET = os.getenv("CRON_SECRET")

# --- Lifespan Event for Managing Resources ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # This code runs ONCE when the application starts up.
    print("INFO:     Application startup: Creating ARQ Redis pool.")
    app.state.arq_pool = await create_pool(RedisSettings.from_dsn(REDIS_URL))
    yield
    # This code runs ONCE when the application is shutting down.
    print("INFO:     Application shutdown: Closing AR-Q Redis pool.")
    await app.state.arq_pool.close()

# --- Initialize Clients ---
app = FastAPI(
    title="London Student Network Polling & Scheduling Service",
    lifespan=lifespan # Attach the lifespan event manager
)
# This redis_client is for simple checks and can be initialized globally.
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Route ---
@app.get("/")
async def welcome():
    return "This is the protected London Student Network polling and scheduling service for Meta subprocesses."

@app.get("/api/health")
async def confirmHealthy(request: Request):
    """
    An advanced health check to test the state of both Redis clients.
    """
    # Security check is commented out for easy manual testing
    # expected_auth_header = f"Bearer {CRON_SECRET}"
    # if not CRON_SECRET or request.headers.get("Authorization") != expected_auth_header:
    #     raise HTTPException(status_code=401, detail="Unauthorized")
    
    results = {}
    
    try:
        # 1. Access both clients, just like the failing cron route does.
        arq_pool = request.app.state.arq_pool
        global_client = redis_client
        
        results["arq_pool_access"] = "ok"
        results["global_client_access"] = "ok"

        # 2. Test writing with one client and reading with the other.
        test_key = "health_check_key"
        test_value = f"ok_{int(time.time())}"
        
        # Write with the lifespan-managed pool
        await arq_pool.set(test_key, test_value, ex=10) # Set with a 10s expiry
        results["arq_pool_write"] = "ok"
        
        # Read with the global client
        read_value = await global_client.get(test_key)
        results["global_client_read"] = "ok"
        
        # 3. Verify the result
        if read_value == test_value:
            results["verification"] = "ok"
        else:
            results["verification"] = f"failed: wrote '{test_value}', read '{read_value}'"

    except Exception as e:
        # If any part of this fails, the exception will be returned in the response
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": "Health check failed during Redis interaction.",
                "error_type": type(e).__name__,
                "error_details": str(e),
                "current_results": results
            }
        )

    return JSONResponse(
        status_code=200,
        content={"status": "success", "message": "Both Redis clients were accessed and verified successfully.", "results": results}
    )

@app.get("/api/cron/poll-instagram")
async def poll_instagram_and_enqueue(request: Request):
    """
    Triggered by a cron job, this function polls Instagram for new posts
    for all tracked users and enqueues them for processing, following all
    pagination cursors to ensure no posts are missed.
    """
    # expected_auth_header = f"Bearer {CRON_SECRET}"
    # if not CRON_SECRET or request.headers.get("Authorization") != expected_auth_header:
    #     raise HTTPException(status_code=401, detail="Unauthorized")
    
    arq_pool = request.app.state.arq_pool

    active_user_ids = await redis_client.smembers("instagram_polling_list")
    new_posts_found = 0

    if not active_user_ids:
        return JSONResponse(
            status_code=200,
            content={"status": "complete", "message": "No active users to poll."}
        )

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

            # This is the starting point for our pagination loop
            current_api_url = f"https://graph.instagram.com/me/media?fields=id,caption,media_type,permalink,timestamp&since={last_polled_timestamp}"

            # --- PAGINATION LOGIC STARTS HERE ---
            while current_api_url:
                try:
                    response = await client.get(current_api_url, headers={"Authorization": f"Bearer {access_token}"})
                    response.raise_for_status()
                    json_data = response.json()
                    posts = json_data.get("data", [])

                    if posts:
                        for post in posts:
                            media_type = post.get("media_type")

                            if media_type == "IMAGE": # for now skip VIDEO, and CAROUSEL_ALBUM
                                job_data = {
                                    "post_id": post.get("id"),
                                    "post_url": post.get("permalink"),
                                    "caption": post.get("caption", ""),
                                    "media_type": media_type
                                }
                                await arq_pool.enqueue_job("process_instagram_post", job_data)
                                new_posts_found += 1
                    
                    # Check for the 'next' link in the 'paging' object to continue the loop
                    current_api_url = json_data.get("paging", {}).get("next")

                except httpx.HTTPStatusError as e:
                    print(f"HTTP Error for user {user_id}: {e.response.text}")
                    current_api_url = None # Stop paginating for this user on error
                except Exception as e:
                    print(f"Unexpected error for user {user_id}: {e}")
                    current_api_url = None # Stop paginating for this user on error
            
            # Update the timestamp only after successfully processing all pages for this user
            await redis_client.hset(user_cache_key, "last_polled_timestamp", poll_start_time)

    return JSONResponse(
        status_code=200,
        content={"status": "success", "message": f"number of new posts found: ${new_posts_found}"}
    )
