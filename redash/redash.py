import requests, json
import pandas as pd
import gspread


url = "https://dashboard.srcli.xyz/api/queries/9909/results"

res = requests.post(
    url = url,
    headers= {
"content-type": "application/json",
"cookie": "_oauth2_proxy=eyJFbWFpbCI6InJ5YW4uYWRpQHNpcmNsby5jb20iLCJVc2VyIjoiMTE2NjA0MDkzMzY5ODYyMjk5MzY5In0=|1664956324|JVU7-uhaWJoTSshNB8plO_fAe-A=; remember_token=529-87e15556061f75d6ae5159cb7ab5d73f|80d6ddc77565c3dbd503d4c5b4e52dde8cfae59316552957177d21beabb83eff7af3dd1b88adeb46a50714e906fa0cea784e78afcec1a8f550dc80e48a90e8ae; csrf_token=ImM0ZjhlZWEwZGEyNzExYTI2ZjEwMmM5OTU5ODZkZWYyOGY0Y2JhNzUi.Y0POzQ.Zyeuz53mbeTjy67eHzfjXGp1f9k; session=.eJwNy0sOAiEMANC7dD0mFKf8LkMKtNGoaIBZGe_ubF_yvpB1yLxBUn5O2SB_ZLy4S1-Q1jhOqXNoXu-HdEhQdw0ibBpbj8jWKRpbY6QYXBO1Qfda2BNscEwZ-d7ORDZeghckImccqqfmWAgp1uK5UPNXhd8fRgcqRQ.Y0POzQ.ANBRI2hJP8n7v-_56cqB-CQTwYw"
},
data = json.dumps({"id":9909,"parameters":{},"apply_auto_limit":True,"max_age":0})
    )

res_url = res.json()['job']['id']
print(res)
while True:
    res = requests.get(
        url = f'https://dashboard.srcli.xyz/api/jobs/{res_url}',
        headers = {"content-type": "application/json",
    "cookie": "_oauth2_proxy=eyJFbWFpbCI6InJ5YW4uYWRpQHNpcmNsby5jb20iLCJVc2VyIjoiMTE2NjA0MDkzMzY5ODYyMjk5MzY5In0=|1664956324|JVU7-uhaWJoTSshNB8plO_fAe-A=; remember_token=529-87e15556061f75d6ae5159cb7ab5d73f|80d6ddc77565c3dbd503d4c5b4e52dde8cfae59316552957177d21beabb83eff7af3dd1b88adeb46a50714e906fa0cea784e78afcec1a8f550dc80e48a90e8ae; csrf_token=ImM0ZjhlZWEwZGEyNzExYTI2ZjEwMmM5OTU5ODZkZWYyOGY0Y2JhNzUi.Y0POzQ.Zyeuz53mbeTjy67eHzfjXGp1f9k; session=.eJwNy0sOAiEMANC7dD0mFKf8LkMKtNGoaIBZGe_ubF_yvpB1yLxBUn5O2SB_ZLy4S1-Q1jhOqXNoXu-HdEhQdw0ibBpbj8jWKRpbY6QYXBO1Qfda2BNscEwZ-d7ORDZeghckImccqqfmWAgp1uK5UPNXhd8fRgcqRQ.Y0POzQ.ANBRI2hJP8n7v-_56cqB-CQTwYw"}
    )
    
    if res.json()['job']['result'] != None:
        break

res_id = res.json()['job']['result']

res = requests.get(
        url = f'https://dashboard.srcli.xyz/api/query_results/9613392.json',
        headers = {"content-type": "application/json",
    "cookie": "_oauth2_proxy=eyJFbWFpbCI6InJ5YW4uYWRpQHNpcmNsby5jb20iLCJVc2VyIjoiMTE2NjA0MDkzMzY5ODYyMjk5MzY5In0=|1664956324|JVU7-uhaWJoTSshNB8plO_fAe-A=; remember_token=529-87e15556061f75d6ae5159cb7ab5d73f|80d6ddc77565c3dbd503d4c5b4e52dde8cfae59316552957177d21beabb83eff7af3dd1b88adeb46a50714e906fa0cea784e78afcec1a8f550dc80e48a90e8ae; csrf_token=ImM0ZjhlZWEwZGEyNzExYTI2ZjEwMmM5OTU5ODZkZWYyOGY0Y2JhNzUi.Y0POzQ.Zyeuz53mbeTjy67eHzfjXGp1f9k; session=.eJwNy0sOAiEMANC7dD0mFKf8LkMKtNGoaIBZGe_ubF_yvpB1yLxBUn5O2SB_ZLy4S1-Q1jhOqXNoXu-HdEhQdw0ibBpbj8jWKRpbY6QYXBO1Qfda2BNscEwZ-d7ORDZeghckImccqqfmWAgp1uK5UPNXhd8fRgcqRQ.Y0POzQ.ANBRI2hJP8n7v-_56cqB-CQTwYw"}
    )
    
print(res)
pretty_json = json.loads(res.text)
# print(json.dumps(pretty_json['query_result']['data']['rows'], indent=4))
df = res.json()['query_result']['data']['rows']


