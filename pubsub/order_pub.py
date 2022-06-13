import json
from google.cloud import pubsub_v1
from sqlalchemy import false
from google.oauth2 import service_account
from datetime import datetime, timedelta

# --- set credential --- #
credentials = service_account.Credentials.from_service_account_file(
    'credentials/service_acc.json')
current_time = lambda: datetime.utcnow() + timedelta(hours=+7)
# --- define some variables --- #
topic_path = 'projects/ryan-99/topics/test'

# --- simple message body --- #
# data = 'pesanan masuk'
# data = data.encode('utf-8')

# --- message body using json format --- #
data = {
    'sku': 'AU1234',
    'qty': 9,
    'color': f'{current_time()}',
    'is_promo': True,
    # 'test':False
}
data = json.dumps(data).encode('utf-8')

# --- message attributes --- #
attributes = {
    'sku': 'AU123',
    'qty': '5',
    'color': 'black',
    'is_promo': 'false'
}

# --- create subscriber client and publishing the message --- #
publisher = pubsub_v1.PublisherClient(credentials=credentials)
# future = publisher.publish(topic_path, data)
future = publisher.publish(topic_path, data, **attributes)
print(f'published message id {future.result()}')

