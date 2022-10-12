from google.cloud import bigquery
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os, datetime, json
from google.api_core.exceptions import BadRequest

env_path = Path('.') / '.env'
load_dotenv(dotenv_path = env_path)

dataset = os.environ['DATASET']
table   = os.environ['TABLE']
cred    = os.environ['CREDENTIALS']

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred

client = bigquery.Client()

dataset_ref = client.dataset(dataset)
table_ref   = dataset_ref.table(table)

def load_to_bq(result, done_ts):
    bot_chat_log = [key for key in result['messages'] if 'bot_id' in key] 
    chat_log = [key for key in result['messages'] if 'bot_id' not in key]

    data = json.dumps({
        'created_timestamp' : bot_chat_log[0]['ts'],
        'requestor' : result["messages"][0]["blocks"][1]["text"]["text"][10:-1],
        'entity' : result["messages"][0]["blocks"][2]["text"]["text"][10:],
        'department' : result["messages"][0]["blocks"][3]["text"]["text"][14:],
        'brand_team_product' : result["messages"][0]["blocks"][4]["text"]["text"][9:],
        'request' : result["messages"][0]["blocks"][5]["text"]["text"][11:],
        'related_link' : result["messages"][0]["blocks"][6]["text"]["text"][19:],
        'other' : result["messages"][0]["blocks"][7]["text"]["text"][10:],
        'expected_due' : result["messages"][0]["blocks"][8]["text"]["text"][18:-1],
        'approved_timestamp' : bot_chat_log[1]['ts'],
        'in_progress_timestamp' : bot_chat_log[3]['ts'],
        'user_review_timestamp' : bot_chat_log[4]['ts'],
        'done_timestamp' : done_ts,
        'chat_log' : [dict(user=log['user'], timestamp=log['ts'], text=log['text']) for log in chat_log]
    })
    # print(df)
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("created_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("requestor", "STRING"),
            bigquery.SchemaField("entity", "STRING"),
            bigquery.SchemaField("department", "STRING"),
            bigquery.SchemaField("brand_team_product", "STRING"),
            bigquery.SchemaField("request", "STRING"),
            bigquery.SchemaField("related_link", "STRING"),	
            bigquery.SchemaField("other", "STRING"),
            bigquery.SchemaField("expected_due", "STRING"),
            bigquery.SchemaField("approved_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("in_progress_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("user_review_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("done_timestamp", "TIMESTAMP"),
            bigquery.SchemaField(
                "chat_log", 
                "RECORD", 
                mode = "REPEATED",
                fields = [
                    bigquery.SchemaField("user", "STRING"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("text", "STRING")
                ]
            )
        ],
        autodetect=False,
        source_format = 'NEWLINE_DELIMITED_JSON'
    )
    
    data = json.loads(data)

    try:
        job = client.load_table_from_json([data], table_ref, job_config=job_config)
        job.result()
    except BadRequest:
        for error in job.errors:
            print(error)
