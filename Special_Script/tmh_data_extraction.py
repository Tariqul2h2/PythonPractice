import json
import requests
from sys import argv
import pandas as pd
import os
import boto3
from requests_aws4auth import AWS4Auth

'''Sample Input: python local_data.py ecommerce_data'''

Index_name = argv[1]
# authentication
region = 'eu-west-1'
service = 'es'

# staging
host_address = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'
# # prod
# host_address = 'vpc-the-mobilityhouse-dpxinpje6ppvrbithyefgxwq7a.eu-west-1.es.amazonaws.com'


header = {
    'Content-Type': 'application/json'
}

payload = {
  "size": 10000,
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "device_id": "68d8c31c4ae09cb4dbdf5ee02a97bbe7"
          }
        }
      ],
      "filter": {
        "range": {
          "@timestamp": {
            "time_zone": "+06:00",
            "gte": "2022-03-03T00:00:00.000",
            "lte": "2022-03-30T23:59:00.000"
          }
        }
      }
    }
  }
}

scroll_payload = {
    "scroll" : "1m",
    "scroll_id" : None
}
scroll_path = '_search/scroll'
index_path = Index_name + '*/_search?scroll=1m'

# url = f'https://{host_address}/{index_path}'
url = 'https://' + host_address + '/' + index_path


file_count = 0
count = 0

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

try:
    if not os.path.exists('files'):
        os.makedirs('files')
except OSError as e:
    print(str(e))
    print('unable to create folder')
    raise Exception('Unable to create folder "files" to store results.')

try:
    response = requests.get(url, auth=awsauth, headers=header, data=json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']
    temp_file = [d['_source'] for d in data]
    df = pd.DataFrame(temp_file)
    file_name = 'files/file_{}.csv'.format(file_count)
    df.to_csv(file_name,index=None)
    print(file_name)
    count = len(data)
    file_count = file_count + 1
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    # print(scroll_payload)
    # url = f'https://{host_address}/{scroll_path}'
    url = 'https://' + host_address + '/' + scroll_path

    while len(query_response['hits']['hits']):
        response = requests.get(url, auth=awsauth, headers=header, data=json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']
        temp_data = [d['_source'] for d in data]
        df = pd.DataFrame(temp_data)
        file_name = 'files/file_{}.csv'.format(file_count)
        print(file_name)
        df.to_csv(file_name,index=None)
        file_count = file_count + 1
        scroll_payload['scroll_id'] = query_response['_scroll_id']
        count = count + len(data)
    print(count)
except Exception as e:
    print('Error is ',e)