import os
import re
import json
import argparse
from socket import timeout
from dotenv import load_dotenv
from glob import glob
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# ------------ Initiate argument in command ------------
load_dotenv()
cred            = os.getenv('CREDENTIALS')
project_id      = os.getenv('PROJECT_ID')
topic_id        = os.getenv('TOPIC_ID')
subscription_id = os.getenv('SUBSCRIPTION_ID')
bucket          = os.getenv('BUCKET')
bucket_folder   = os.getenv("BUCKET_FOLDER")
dataset         = os.getenv('DATASET')
table           = os.getenv('TABLE')
dataset_error   = os.getenv('DATASET_ERROR')
table_error     = os.getenv('TABLE_ERROR')
timeout         = int(os.getenv('TIMEOUT'))

# ------------ Set env ------------
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred

# ------------ Set variable ------------
current_time      = lambda: datetime.utcnow().isoformat(' ')

bq_client         = bigquery.Client()
dataset_ref       = bq_client.dataset(dataset)
table_ref         = dataset_ref.table(table)

dataset_error_ref = bq_client.dataset(dataset_error)
table_error_ref   = dataset_ref.table(table_error)

gcs_client        = storage.Client(project = project_id)
bucket            = gcs_client.get_bucket(bucket)

publisher         = pubsub_v1.PublisherClient()
project_path      = publisher.common_project_path(project_id)
topic_path        = publisher.topic_path(project_id, topic_id)
prefix_temp_file  = f'{project_id}.{topic_id}.{subscription_id}'

def run():
    while True:
        # ------------ Load Temp File to BQ ------------
        blobs       = bucket.list_blobs(prefix=f'{bucket_folder}/temp_pubsub/')
        
        for filename in blobs:
            # read same file because if use 1 then return error 'stream must be at beginning.'
            with filename.open('rb') as file:
                file_lines = file.readlines()
                file.close()

            with filename.open('rb') as file:
                try:
                    job_config = bigquery.LoadJobConfig(
                        max_bad_records = len(file_lines),
                        source_format   = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        schema          = [
                            bigquery.SchemaField("transactionId", "INTEGER"),
                            bigquery.SchemaField("warehouseId", "STRING"),
                            bigquery.SchemaField("channelId", "STRING"),
                            bigquery.SchemaField("sku", "STRING"),
                            bigquery.SchemaField("delta", "INTEGER"),
                            bigquery.SchemaField("quantity", "INTEGER"),
                            bigquery.SchemaField("version", "INTEGER"),
                            bigquery.SchemaField("changedByAccountId", "STRING"),
                            bigquery.SchemaField("changedByAccountName", "STRING"),
                            bigquery.SchemaField("timestamp", "TIMESTAMP")
                        ]
                    )

                    load_job = bq_client.load_table_from_file(file, table_ref, job_config=job_config)            
                    load_job.result()
                    error_logs = load_job.errors
                    if error_logs is None:
                        print(f'Load to Bigquery Successful. Table: {table_ref}')

                    # if errors exist, load bad records and its error message to table error       
                    if error_logs:
                        file_lines_str = "".join([line.decode('utf-8') for line in file_lines])
                        error_messages = []

                        for log_index, log in enumerate(error_logs):
                            message = {
                                'timestamp'    : current_time(),
                                'payloadString': None,
                                'errorMessage' : json.dumps(log)
                            }
                            
                            # error only return index (we have to extract the message) of text, not lines
                            positions = re.findall(r'JSON parsing error in row starting at position \d+', log['message'])
                            if positions:
                                position = int(re.findall(r'\d+', positions[0])[0])
                                payload  = file_lines_str[position:position+file_lines_str[position:].find('\n')]
                                message['payloadString'] = payload

                            error_messages.append(message)
                        
                        bq_client.load_table_from_json(error_messages, table_error_ref, job_config=bigquery.LoadJobConfig(autodetect=True)).result()
                        print(f'Load Error to Bigquery Successful. Table: {table_error_ref}')

                    # if success, remove current file
                    file.close()
                    filename.delete()
                except Exception as err:
                    print(err)
                    file.close()

        # ------------ Set local variable ------------
        subscriber        = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)

        # ------------ Initate callback ------------
        # write to temp file and ack after message received

        dfs = []
        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            data        = bytes(message.data).decode('utf-8')
            dfs.append(data)
            message.ack()
        
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

        # ------------ Pull pubsub ------------
        with subscriber:    
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()

        print(f'Pull Subscription Datas Successful. Topic: {topic_path}')

        temp_file   = f'temp_pubsub/{prefix_temp_file}.{current_time()}.json'
        blob        = bucket.blob(f'{bucket_folder}/{temp_file}')
        f           = blob.open(mode = 'w')
        f.write('\n'.join([line for line in dfs]))
        f.close()

        print(f'Write data successfull {blob}')

if __name__ == "__main__":
    run()