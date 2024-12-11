import os
import json
from datetime import datetime
import re
from openai import OpenAI
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")  # This is the default and can be omitted
)

def open_ai_call(dataframes):
    try:
        with open('test.json', 'r') as file:
            data = json.load(file)
            # print(data)
        json_format = data['json_format']
        static_prompts = data['static_prompts']
        result = {}
        for i in range(1, 6):
            df = dataframes[f'df_{i}']

            # Convert DataFrame to CSV format
            csv_data = df.to_csv(index=False)
            today_date = datetime.now().strftime("%Y-%m-%d")
            prompt = static_prompts.format(today_date=today_date, json_format=json.dumps(json_format), csv_data=csv_data)
            # print(prompt)
            # Define the messages
            messages = [
                {
                    "role": "system",
                    "content": "You are a data analyst. You have to analyze the data given below and give the response based on the question asked by the user."
                },

                {
                    "role": "user",
                    "content": prompt
                }
            ]

            # Use OpenAI client to send the request
            chat_completion = client.chat.completions.create(
                messages=messages,
                model="gpt-4o",
                temperature=0.01
            )
            # print(chat_completion)
            # Extract the content from the response
            content = chat_completion.choices[0].message.content
            content_cleaned = content.strip()
            content_cleaned = re.sub(r"^```(?:json)?|```$", "", content.strip(), flags=re.DOTALL).strip()

            # print(content_cleaned)
            try:
                content_cleaned = json.loads(content_cleaned)
                print(content_cleaned)
                result[f'df_{i}'] = content_cleaned
            except Exception as e:
                logger.error(f"Error occurred during post processing: {e}")
        logger.info("Open AI call completed successfully")
    except Exception as e:
        logger.error(f"Error occurred during open_ai_call: {e}")
    return result