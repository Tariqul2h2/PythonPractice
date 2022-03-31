import os
import curator
import json
import sys
import csv
import boto3
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
region = 'eu-west-1'
service = 'es'
#staging
#host = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'
# # prod
host = 'vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'
index_name = 'device_status-*'
headers = {"Content-Type": "application/json"}
scroll_path = '_search/scroll'
payload = {
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "device_id": "d083a2d4e5c78de29c1dbb24f5a83495"
          }
        }
      ],
      "filter": {
        "range": {
          "@timestamp": {
            "gte": "2021-04-29",
            "lte": "2021-05-01"
          }
        }
      }
    }
  },
  "_source": [
    "@timestamp",
    "chargers"
  ]
}
scroll_payload = {
    "scroll": "1m",
    "scroll_id": None
}
def to_csv(data, file_name):
    try:
        doc_count = len(data)
        with open(file_name, 'w') as file:
            temp_data = [[d['_source'].get('@timestamp'), 
            		d['_source'].get('chargers')] for d in data]
            temp_data.insert(0, ['@timestamp',
            			'chargers'])
            csv_writer = csv.writer(file)
            csv_writer.writerows(temp_data)
        print('{} inserted {} data in this batch'.format(file_name, doc_count))
        return doc_count
    except Exception as e:
        print(str(e))
        raise Exception('Unable to insert data.')
def main():
    # create folder to store result files
    try:
        if not os.path.exists('files'):
            os.makedirs('files')
    except OSError as e:
        print(str(e))
        print('unable to create folder')
        raise Exception('Unable to create folder "files" to store results.')
    file_count = 0
    # get authentication credentials
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    file_name = 'files/file_{}.csv'.format(file_count)
    file_count = file_count + 1
    total_count = 0
    # part 1: get first 10000 data and scroll_id
    index_path = index_name + '/_search?scroll=1m'
    url = 'https://' + host + '/' + index_path
    response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']  # extracting the first page
    move_count = to_csv(data, file_name)
    total_count = total_count + move_count
    # Step 2: Request to scroll with the scroll_id
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    url = 'https://' + host + '/' + scroll_path
    while len(query_response['hits']['hits']):
        response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']  # extracting the next 10000 data
        file_name = 'files/file_{}.csv'.format(file_count)
        file_count = file_count + 1
        move_count = to_csv(data, file_name)
        total_count = total_count + move_count
        scroll_payload['scroll_id'] = query_response['_scroll_id']
    print('total data inserted {}'.format(total_count))
if __name__ == '__main__':
    main()

