import os
import json
from arq import create_pool
from arq.connections import RedisSettings
from google import genai
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import datetime, timezone

# redeploy
# --- Environment Variable Setup ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "a-secret-verify-token")

# --- Initialize Clients ---
app = FastAPI()
# CORRECTED: Typo `Non` changed to `None`
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
# --- ARQ (Task Queue) Configuration ---
# Using the constructor directly to initialize from a URL string.
ARQ_REDIS_SETTINGS = RedisSettings.from_dsn(dsn=REDIS_URL)

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
# This function defines the job that will be run by our queue worker.
async def process_instagram_post(ctx, job_data: dict):
    """
    This is the core task function. It takes a job from the queue,
    calls the Gemma model, and processes the result.
    ARQ handles retries automatically based on the worker configuration.
    """
    post_url = job_data.get("post_url")
    if not gemini_client or not post_url:
        return "Clients not configured or post_url missing, job failed."

    try:
        placeholder_caption = f"This is a sample event post for {post_url}. Event: Tech Meetup on 2025-10-15 at 18:00 at The Innovation Hub, hosted by LSN."
        
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
        
        Caption: "{placeholder_caption}"
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
            # We can use the redis client provided by the ARQ context
            redis_client = ctx['redis']
            await redis_client.hset("processed_events", post_url, event_data.model_dump_json())
        
        return f"Successfully processed {post_url}. Event detected: {event_data.is_event}"

    except Exception as e:
        print(f"Failed to process job for {post_url}. Error: {e}")
        # By raising the exception, we tell ARQ that the job failed,
        # and it will handle the retry logic automatically.
        raise

# --- ARQ Worker Settings Class ---
# This class tells ARQ which functions are available to be run as tasks.
class WorkerSettings:
    functions = [process_instagram_post]
    redis_settings = ARQ_REDIS_SETTINGS
    # Automatically retry failed jobs up to 2 times (3 attempts total)
    max_tries = 3

# --- API Routes ---

@app.api_route("/api/instagram/webhook", methods=["GET", "POST"])
async def instagram_webhook(request: Request):
    """
    Handles Instagram Webhook verification and enqueues new post events into ARQ.
    """
    if request.method == "GET":
        verify_token = request.query_params.get("hub.verify_token")
        if verify_token == WEBHOOK_VERIFY_TOKEN:
            challenge = request.query_params.get("hub.challenge")
            return Response(content=challenge, media_type="text/plain")
        raise HTTPException(status_code=403, detail="Verification token mismatch")

    if request.method == "POST":
        data = await request.json()
        if data.get("object") == "instagram" and data.get("entry"):
            arq_pool = await create_pool(ARQ_REDIS_SETTINGS)
            for entry in data["entry"]:
                for change in entry.get("changes", []):
                    if change.get("field") == "media":
                        media_id = change["value"]["media_id"]
                        post_url = f"https://www.instagram.com/p/{media_id}/"
                        job_data = {"post_url": post_url}
                        # Enqueue the job for our worker to process
                        await arq_pool.enqueue_job("process_instagram_post", job_data)
        return JSONResponse(content={"status": "success"}, status_code=200)

    return JSONResponse(content={"error": "Method not allowed"}, status_code=405)

@app.get("/api/health")
async def health_check():
    return "I'm healthy :)"

@app.post("/api/cron/process-queue")
async def cron_process_queue():
    """
    Triggered by a cron job, this route acts as a serverless "burst" worker.
    It will process a batch of jobs from the queue and then exit.
    """
    from arq.worker import Worker

    worker = Worker(
        functions=[process_instagram_post], 
        redis_settings=ARQ_REDIS_SETTINGS,
        max_jobs=6  # Process a maximum of 6 jobs to respect the 6 RPM limit
    )
    await worker.run_check() # run_check processes jobs and exits when the queue is empty or max_jobs is reached
    
    return JSONResponse(content={
        "message": f"Cron job executed. Processed up to {worker.jobs_complete} jobs."
    })
