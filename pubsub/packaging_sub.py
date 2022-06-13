import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from google.oauth2 import service_account

# --- set credential --- #
credentials = service_account.Credentials.from_service_account_file(
    'credentials/service_acc.json')

# --- define some variables --- #
subscription_path = 'projects/ryan-99/subscriptions/test-sub'
# timeout = 5.0 # timeout in seconds

def callback(message):
    print(f'Received message: {message} with message id {message.message_id}')    
    decode_msg = message.data.decode("utf-8")
    # print(f'data: {decode_msg}')

    # --- convert decoded message into json --- #
    json_msg = json.loads(decode_msg)
    print(f'data: {json_msg["sku"]}')

    # --- using message attributes --- #
    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f"{key}: {value}")

    # --- ack the messages --- #
    message.ack()
    print(f"Acknowledged message id {message.message_id}.")           

# --- create subscriber client and start to listen --- #
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f'Listening for messages on {subscription_path}')

with subscriber:                                                # wrap subscriber in a 'with' block to automatically call close() when done
    try:
        # streaming_pull_future.result(timeout=timeout)
        streaming_pull_future.result()                          # going without a timeout will wait & block indefinitely
    except TimeoutError:
        streaming_pull_future.cancel()                          # trigger the shutdown
        streaming_pull_future.result()                          # block until the shutdown is complete
