import os
import json
import httpx
from arq import create_pool
from arq.connections import RedisSettings
from google import genai
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import datetime, timezone
import time
import redis.asyncio as redis

# --- Environment Variable Setup ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Initialize Clients ---
app = FastAPI()
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# CORRECTED: Using the proper from_dsn class method for ARQ settings
ARQ_REDIS_SETTINGS = RedisSettings.from_dsn(dsn=REDIS_URL)

# CORRECTED: Creating a separate, dedicated redis client for general application use
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


# --- CORS Middleware ---
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models for Type Safety ---
class EventDetails(BaseModel):
    is_event: bool
    title: Optional[str] = None
    date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    location: Optional[str] = None
    host: Optional[str] = None

# --- ARQ Worker Function ---
async def process_instagram_post(ctx, job_data: dict):
    """
    This is the core task function. It takes a job from the queue,
    calls the Gemma model, and processes the result.
    """
    post_url = job_data.get("post_url")
    post_caption = job_data.get("caption")

    if not gemini_client or not post_url or not post_caption:
        return "Clients not configured or job data is missing."

    try:
        prompt = f"""
        Analyze the following Instagram post caption to determine if it describes an event.
        If it is an event, extract the details. If a detail is not present, use null.
        - is_event: boolean
        - title: The official title of the event.
        - date: The date in YYYY-MM-DD format.
        - start_time: The start time in HH:MM (24-hour) format.
        - end_time: The end time in HH:MM (24-hour) format.
        - location: The physical address or venue name.
        - host: The name of the organization hosting.
        
        Caption: "{post_caption}"
        """
        
        response = await gemini_client.aio.models.generate_content(
            model="models/gemma-3-4b-it",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": EventDetails,
            },
        )

        parsed_response = response.parsed

        # CORRECTED: Added a runtime type check to satisfy Pylance and ensure type safety.
        if not isinstance(parsed_response, EventDetails):
            # If the model returns something other than our Pydantic model, it's an error.
            raise TypeError(f"LLM returned an unexpected type: {type(parsed_response)}")
        
        event_data: EventDetails = parsed_response
        
        if event_data and event_data.is_event:
            redis_in_ctx = ctx['redis']
            await redis_in_ctx.hset("processed_events", post_url, event_data.model_dump_json())
        
        return f"Successfully processed {post_url}. Event detected: {event_data.is_event}"

    except Exception as e:
        print(f"Failed to process job for {post_url}. Error: {e}")
        raise

# --- ARQ Worker Settings Class ---
class WorkerSettings:
    functions = [process_instagram_post]
    redis_settings = ARQ_REDIS_SETTINGS
    max_tries = 3

# --- API Routes ---

@app.get("/api/cron/run-pipeline")
async def cron_run_pipeline():
    """
    This single cron job handles the entire pipeline:
    1. Polls Instagram for new posts for all active users based on Redis data.
    2. Enqueues new posts into the ARQ worker queue.
    3. Triggers the ARQ worker to process a batch of jobs.
    """
    
    # --- POLLING PHASE ---
    # Get the list of all users to poll from our fast Redis Set (our source of truth)
    active_user_ids = await redis_client.smembers("instagram_polling_list")
    new_posts_found = 0
    
    if not active_user_ids:
        return JSONResponse(content={"polling_summary": "No active users to poll."})

    async with httpx.AsyncClient() as client:
        arq_pool = await create_pool(ARQ_REDIS_SETTINGS)
        for user_id in active_user_ids:
            user_cache_key = f"user:{user_id}:instagram"
            # Fetch the user's data directly from Redis cache.
            cached_data = await redis_client.hgetall(user_cache_key)
            
            access_token = cached_data.get("access_token")
            last_polled_timestamp = cached_data.get("last_polled_timestamp")

            if not access_token:
                print(f"Warning: No access token found in cache for user {user_id}. Skipping.")
                continue
            
            # Poll Instagram API for new posts since the last check
            api_url = f"https://graph.instagram.com/me/media?fields=id,caption,permalink,timestamp"
            if last_polled_timestamp:
                api_url += f"&since={last_polled_timestamp}"

            try:
                response = await client.get(api_url, headers={"Authorization": f"Bearer {access_token}"})
                response.raise_for_status()
                posts = response.json().get("data", [])

                if posts:
                    for post in posts:
                        job_data = {"post_url": post["permalink"], "caption": post.get("caption", "")}
                        await arq_pool.enqueue_job("process_instagram_post", job_data)
                        new_posts_found += 1
                
                # Update the last polled timestamp in Redis for this user
                await redis_client.hset(user_cache_key, "last_polled_timestamp", int(time.time()))

            except httpx.HTTPStatusError as e:
                # This could indicate an expired token. The user would need to reconnect.
                print(f"Error polling for user {user_id}: {e.response.text}")
            except Exception as e:
                print(f"An unexpected error occurred during polling for user {user_id}: {e}")

    # --- PROCESSING PHASE ---
    from arq.worker import Worker
    worker = Worker(
        functions=[process_instagram_post], 
        redis_settings=ARQ_REDIS_SETTINGS,
        max_jobs=6
    )
    await worker.run_check()
    
    return JSONResponse(content={
        "polling_summary": f"Found and enqueued {new_posts_found} new posts for {len(active_user_ids)} users.",
        "processing_summary": f"Processed up to {worker.jobs_complete} jobs from the queue."
    })

# seed redis for testing
# re-read code
# put some test logs

# PROD
# encrypt tokens
# update database table fields
# connect to lsn database
# update redis in lsn code
