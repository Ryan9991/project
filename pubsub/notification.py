import json
import sys
import random
import requests
import time

def slack_hook(data_err, message):
    url     = 'https://hooks.slack.com/services/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' 
    message = message
    title   = 'Inventory log load failed'
    slack_data = {
        "username"   : "Atlas PubSub",
        "mrkdwn_in"  : ["text"],
        "icon_emoji" : ":satellite:",
        "channel"    : "#testing_pubsub",
        "color"      : "#FF0000",
        "fields"     : [
                    {
                        "title": f"{title}",
                        "value": f"{message} \n Data error : {data_err}",
                        "short": "false"
                    }
                ]
        ,
        "ts"        : str(time.time())
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    print(json.dumps(message))
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)