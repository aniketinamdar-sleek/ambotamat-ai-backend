from loguru import logger
from data_processing import run
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def index():
    return "App is running"

@app.get("/api/v1/extract_data")
async def execute_run():
    logger.info("Running the extraction process")
    result = run()
    return result

