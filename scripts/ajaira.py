import os
from traceback import print_tb
import curator
import json
import sys
import csv
import boto3
import time
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
region = 'eu-west-1'
service = 'es'
# staging
#host = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'
# # prod
host = 'vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'
index_name = 'device_status-v3.0.0-2022.01.20'
headers = {"Content-Type": "application/json"}
scroll_path = '_search/scroll'
payload = {
    "size": 10000,
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "device_id": "4a085b6a6e7edfb1cd953fc62408c08d"
          }
        },
        {
          "match": {
            "device_id": "f30244cbf3183340a078f301466e3cbd"
          }
        },
        {
          "match": {
            "device_id": "9b896ca11166ffa30fcfe01daa300279"
          }
        },
        {
          "match": {
            "device_id": "a104874f52592ac72b9679a355c98009"
          }
        },
        {
          "match": {
            "device_id": "37356f065cc2923fe394a1b601981f9c"
          }
        },
        {
          "match": {
            "device_id": "3fa44e4c4bbfe6e0a0ed1d024727302a"
          }
        },
        {
          "match": {
            "device_id": "ed472d0e202e0a8db1c0a68877469d08"
          }
        },
        {
          "match": {
            "device_id": "f458b3dd2c91edaaad35c2a937090610"
          }
        },
        {
          "match": {
            "device_id": "db0c4028962cdd878a73447b98443d45"
          }
        },
        {
          "match": {
            "device_id": "ad9e5daaf14aa3e87a9b54b3ae91bffd"
          }
        },
        {
          "match": {
            "device_id": "a50a7024755666b9f827b1f3d364d3b4"
          }
        },
        {
          "match": {
            "device_id": "3304b4d3b93fb65f69c827cf009b27e2"
          }
        }
      ]
    }
  }
  # , "_source": ["@timestamp", "evs"]
}
scroll_payload = {
    "scroll": "1m",
    "scroll_id": None
}
def to_csv(data, file_name):
    try:
        doc_count = len(data)
        print(doc_count)
        with open(file_name, 'w') as file:
            temp_data = [d for d in data]
#            print(temp_data)
            # temp_data = [[d['_source'].get('@timestamp'), d['_source'].get('evs')]  for d in data]
            temp_data.insert(0, ['@timestamp', 'evs'])
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
    t0 = time.time()
    response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']  # extracting the first page
    print(data)
    move_count = to_csv(data, file_name)
#    t1 = time.time()
    total_count = total_count + move_count
#    print('Total Time: ',t1-t0)
    # Step 2: Request to scroll with the scroll_id
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    url = 'https://' + host + '/' + scroll_path
    while len(query_response['hits']['hits']):
        response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']  # extracting the next 10000 data
        print(data)
        file_name = 'files/file_{}.csv'.format(file_count)
        file_count = file_count + 1
        move_count = to_csv(data, file_name)
        total_count = total_count + move_count
        scroll_payload['scroll_id'] = query_response['_scroll_id']
    print('total data inserted {}'.format(total_count))
    t1 = time.time()
    print('Total time:',t1-t0)
if __name__ == '__main__':
    main()

