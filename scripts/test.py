import os
import curator
import json
import sys
import csv
import boto3
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from elasticsearch_dsl import Search, A
from pprint import pprint

# authentication
# prod
host = 'vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'
region = 'eu-west-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

client = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def main():
    # s = Search(using=client, index="tmh_sc_site-*") \
    #     .query("match", device_id="902a5cd07631eb71e2005fcc84125c8f") \
    #     .filter("range", **{"@timestamp": {"gte": "2021-11-08","lte": "2021-11-09"}})

    # s = s.extra(track_total_hits=True)
    # s = s.extra(size=5)
    # s = s.source(['device_id', 'site_id'])
    # response = s.execute()
    # print(s.to_dict())
    # print(response.hits.total.value)
    # for hit in response:
    #     print(hit.to_dict())

    # total_data = [hit.to_dict() for hit in s.scan()]
    # print(len(total_data))
    # with open("data_using_library.json", 'w') as f:
    #     json.dump(total_data, f)

    s = Search(using=client, index="device_status-*").params(request_timeout=30)
    # s = s.params(request_timeout=30)
    a = A('terms', field='_id', size=10000)
    s.aggs.bucket('unique_device', a)
    # s.aggs.bucket('unique_device', a)
    pprint(s.to_dict())
    response = s.execute()
    print(response)
    for hit in response.aggregations.unique_device.buckets:
        # print(hit.to_dict())
        print(hit.key)


if __name__ == '__main__':
    main()
