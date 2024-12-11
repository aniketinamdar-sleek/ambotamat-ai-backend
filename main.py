from loguru import logger
import uvicorn
from data_processing import run
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allow specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.get("/")
async def index():
    return "App is running"

@app.get("/api/v1/extract_data")
async def execute_run():
    logger.info("Running the extraction process")
    result = run()
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
