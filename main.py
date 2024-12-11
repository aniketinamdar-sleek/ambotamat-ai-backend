from google.cloud import bigquery
import uvicorn
from queries import generate_query
import pandas as pd
import time
import os
from dotenv import load_dotenv
from loguru import logger
import json

import re
from datetime import datetime
from fastapi import FastAPI
from open_ai import open_ai_call

load_dotenv()
service_account_json = os.environ.get("SERVICE_ACCOUNT_KEY")
if service_account_json is None:
    raise ValueError("SERVICE_ACCOUNT_KEY environment variable is not set")
try:
    service_account_info = json.loads(service_account_json, strict=False)
except Exception as e:
    raise ValueError(e)
def get_data_bq():
    try:
        client = bigquery.Client.from_service_account_info(service_account_info)

        # List of queries
        query_list = [generate_query(staff_grouping) for staff_grouping in ['ampoc 1', 'ampoc 2', 'ampoc 3', 'ampoc 4', 'ampoc 5']]
        # Dictionary to store DataFrames
        dataframes = {}

        # Loop through the queries and process them
        for idx, query in enumerate(query_list, start=1):
            # Run the query
            query_job = client.query(query)  # Make an API request.

            # Fetch results and convert to DataFrame
            df = query_job.result().to_dataframe()

            # Save DataFrame in dictionary with a unique key
            df_name = f"df_{idx}"
            dataframes[df_name] = df

            # # Save DataFrame to CSV with a unique name
            # output_csv_path = f"output_query_{idx}.csv"
            # df.to_csv(output_csv_path, index=False)

            # print(f"{df_name} saved to {output_csv_path}")
            time.sleep(2)
        logger.info("Data fetched from BigQuery successfully")
    except Exception as e:
        logger.error(f"Error occurred while fetching data from BigQuery: {e}")
        
    return dataframes



def clean_data(dataframes):
    try:
        dfs = {}
        for i in range(1, len(dataframes) + 1):  # Start from 1 to match the keys
            df = dataframes[f'df_{i}']  # Access the DataFrame using string keys
            df = df.drop_duplicates()
        
        # Process each column based on its dtype
            for column in df.columns:
                if pd.api.types.is_integer_dtype(df[column]):
                    # Fill NaN with 0 for integer columns
                    df[column] = df[column].fillna(0)
                elif pd.api.types.is_datetime64_any_dtype(df[column]):
                    # Fill NaN with NaT for datetime columns
                    df[column] = pd.to_datetime(df[column], errors='coerce')  # Ensure column is datetime
                    df[column] = df[column].fillna(pd.NaT)
                elif pd.api.types.is_float_dtype(df[column]):
                    # Fill NaN with 0.0 for float columns
                    df[column] = df[column].fillna(0.0)
                elif pd.api.types.is_object_dtype(df[column]):
                    # Fill NaN with an empty string for object (string) columns
                    df[column] = df[column].fillna(0)
            
            dfs[f'df_{i}'] = df
        logger.info("Cleaned data completed successfully")  # Save the cleaned DataFrame
    except Exception as e:
        logger.error(f"Error occurred while cleaning data: {e}")
    return dfs

def process_data(dataframes):
    updated_dataframes = {}
    try:
        for i in range(1, 6):  # Loop through all dataframes
            df = dataframes[f'df_{i}']
            
            # Ensure 'next_fye_to_file' is converted to datetime, coercing errors to NaT
            df['next_fye_to_file'] = pd.to_datetime(df['next_fye_to_file'], errors='coerce')
            
            # Apply the 6-month offset to all non-NaT rows in 'next_fye_to_file'
            df.loc[df['next_fye_to_file'].notna(), 'next_fye_to_file'] += pd.DateOffset(months=6)
            
            # Check 'eot_extension' column and apply an additional 2 months if non-empty
            if 'eot_extension' in df.columns:
                condition = df['eot_extension'] != ''  # Check for non-empty values
                df.loc[condition & df['next_fye_to_file'].notna(), 'next_fye_to_file'] += pd.DateOffset(months=2)
            
            # Store the updated DataFrame back
            updated_dataframes[f'df_{i}'] = df
        logger.info("Processed data completed successfully")
    except Exception as e:
        logger.error(f"Error processing DataFrame df_{i}: {e}")
    
    return updated_dataframes



def get_data_preprocess():
    dataframes = get_data_bq()
    dataframes = clean_data(dataframes)
    dataframes = process_data(dataframes)
    return dataframes
    # print(dataframes)
def run():
    dataframes = get_data_preprocess()
    result = open_ai_call(dataframes)
    return result

app = FastAPI()

@app.get("/")
async def index():
    return "App is running"

@app.get("/api/v1/extract_data")
async def execute_run():
    logger.info("Running the extraction process")
    result = run()
    return result

