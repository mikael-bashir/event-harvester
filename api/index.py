from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Define the FastAPI app
app = FastAPI()

# --- CORS Middleware ---
# This allows your Next.js frontend to communicate with this backend.
# In a production environment, you should replace "*" with the actual domain of your frontend app for better security.
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods (GET, POST, etc.)
    allow_headers=["*"], # Allows all headers
)

# --- API Routes ---

@app.get("/api")
async def root():
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "FastAPI backend is active and ready."}

# Define the request body model for type safety
class ColourSwapRequest(BaseModel):
    colour: str

@app.post("/api/swap-colour")
async def swap_colour(request: ColourSwapRequest):
    """
    Swaps the input color between red and green.
    """
    colour = request.colour
    if colour == "red":
        return JSONResponse({"colour": "green"})
    elif colour == "green":
        return JSONResponse({"colour": "red"})
    else:
        return JSONResponse({"error": f"Invalid colour: {colour}. Please use 'red' or 'green'."}, status_code=400)
