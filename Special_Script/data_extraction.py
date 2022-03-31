import json
import requests
from sys import argv
import pandas as pd
import os
'''Sample Input: python local_data.py ecommerce_data'''
Index_name = argv[1]
host_name = 'localhost'
port = 9200
host_address = 'localhost:9200'
header = {
    'Content-Type': 'application/json'
}

payload = {
  "size": 1000, 
  "query": {
    "match_all": {
    }
  },
  "_source": ["customer_full_name","email","geoip"]
}

scroll_payload = {
    "scroll" : "1m",
    "scroll_id" : None
}
scroll_path = '_search/scroll'
index_path = Index_name + '*/_search?scroll=1m'

url = f'http://{host_address}/{index_path}'

file_count = 0
count = 0

try:
    if not os.path.exists('files'):
        os.makedirs('files')
except OSError as e:
    print(str(e))
    print('unable to create folder')
    raise Exception('Unable to create folder "files" to store results.')

try:
    response = requests.get(url, headers = header, data = json.dumps(payload))
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
    url = f'http://{host_address}/{scroll_path}'

    while len(query_response['hits']['hits']):
        response = requests.get(url, headers = header, data = json.dumps(scroll_payload))
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