import os
import curator
import json
import sys
import csv
import boto3
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
#authentication
region = 'eu-west-1'
service = 'es'
​
## staging
# host = 'vpc-staging-mobilityhouse-qzusvj2sfzjyviij4dpv5xa4ma.eu-west-1.es.amazonaws.com'
#prod
host ='vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'
​
index_name = 'tmh_sc_grid_meter-*'
​
headers = {"Content-Type": "application/json"}
scroll_path = '_search/scroll'
payload = {
   "size": 10000,
   "query": {
       "bool": {
           "must": [
               {
                   "match": {
                       "device_id": "c3f7030ae1a2afbd73b91c4e683ed7aa"
                   }
               }
           ],
           "filter": {
               "range": {
                   "@timestamp": {
                       "gte": "now-1M",
                       "lte": "now"
                   }
               }
           }
       }
   },
   "_source": [
       "@timestamp",
       "grid.active_power_phase_1",
       "grid.active_power_phase_2",
       "grid.active_power_phase_3",
       "grid.current_phase_1",
       "grid.current_phase_2",
       "grid.current_phase_3",
       "grid.meter_values_timestamp",
       "grid.power_factor_phase_1",
       "grid.power_factor_phase_2",
       "grid.power_factor_phase_3",
       "grid.reactive_power_phase_1",
       "grid.reactive_power_phase_2",
       "grid.reactive_power_phase_3",
       "grid.total_active_power",
       "grid.total_apparent_power",
       "grid.total_current",
       "grid.total_reactive_power",
       "grid.voltage_phase_1",
       "grid.voltage_phase_2",
       "grid.voltage_phase_3"
   ]
}
scroll_payload = {
   "scroll": "1m",
   "scroll_id": None
}
​
​
def to_csv(data, file_name):
   try:
       doc_count = len(data)
       with open(file_name, 'w') as file:
           temp_data = [[d['_source'].get('@timestamp'),
                         d['_source'].get('grid').get('active_power_phase_1'),
                         d['_source'].get('grid').get('active_power_phase_2'),
                         d['_source'].get('grid').get('active_power_phase_3'),
                         d['_source'].get('grid').get('current_phase_1'),
                         d['_source'].get('grid').get('current_phase_2'),
                         d['_source'].get('grid').get('current_phase_3'),
                         d['_source'].get('grid').get('meter_values_timestamp'),
                         d['_source'].get('grid').get('power_factor_phase_1'),
                         d['_source'].get('grid').get('power_factor_phase_2'),
                         d['_source'].get('grid').get('power_factor_phase_3'),
                         d['_source'].get('grid').get('reactive_power_phase_1'),
                         d['_source'].get('grid').get('reactive_power_phase_2'),
                         d['_source'].get('grid').get('reactive_power_phase_3'),
                         d['_source'].get('grid').get('total_active_power'),
                         d['_source'].get('grid').get('total_apparent_power'),
                         d['_source'].get('grid').get('total_current'),
                         d['_source'].get('grid').get('total_reactive_power'),
                         d['_source'].get('grid').get('voltage_phase_1'),
                         d['_source'].get('grid').get('voltage_phase_2'),
                         d['_source'].get('grid').get('voltage_phase_3')] for d in data]
           temp_data.insert(0, payload["_source"])
           csv_writer = csv.writer(file)
           csv_writer.writerows(temp_data)
       print('{} inserted {} data in this batch'.format(file_name, doc_count))
       return doc_count
   except Exception as e:
       print(str(e))
       raise Exception('Unable to insert data.')
​
​
def main():
   # create folder to store result files
   try:
       if not os.path.exists('sc_grid_meter'):
           os.makedirs('sc_grid_meter')
   except OSError as e:
       print(str(e))
       print('unable to create folder')
       raise Exception('Unable to create folder "files" to store results.')
​
   file_count = 0
   # get authentication credentials
   credentials = boto3.Session().get_credentials()
   awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
   file_name = 'sc_grid_meter/file_{}.csv'.format(file_count)
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
​
   # Step 2: Request to scroll with the scroll_id
   scroll_payload['scroll_id'] = query_response['_scroll_id']
   url = 'https://' + host + '/' + scroll_path
   while len(query_response['hits']['hits']):
       response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(scroll_payload))
       query_response = json.loads(response.text)
       data = query_response['hits']['hits']  # extracting the next 10000 data
       file_name = 'sc_grid_meter/file_{}.csv'.format(file_count)
       file_count = file_count + 1
       move_count = to_csv(data, file_name)
       total_count = total_count + move_count
       scroll_payload['scroll_id'] = query_response['_scroll_id']
​
   print('total data inserted {}'.format(total_count))
​
​
if __name__ == '__main__':
   main()

