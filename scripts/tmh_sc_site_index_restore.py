import argparse
import curator
import json
import datetime
import time
from elasticsearch import Elasticsearch
import boto3
import requests
from requests_aws4auth import AWS4Auth

region = 'eu-west-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
# staging cluster
host = 'vpc-staging-mobilityhouse-qzusvj2sfzjyviij4dpv5xa4ma.eu-west-1.es.amazonaws.com'
headers = {"Content-Type": "application/json"}
payload = {
    "indices": ""
}
start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2020, 1, 1)


def restore_snap_index():
    days = [start_date + datetime.timedelta(n) for n in range(int((end_date - start_date).days))]
    for day in days:
        d1 = day.strftime('%Y-%m-%d')
        d2 = day.strftime('%Y.%m.%d')
        repo_path = "_snapshot/production_charging_snapshot_repo/snapshot_of__{}/_restore".format(d1)
        payload['indices'] = "tmh_sc_site-{}".format(d2)
        url = 'https://' + host + '/' + repo_path
        try:
            r = requests.post(url, auth=awsauth, headers=headers, data=json.dumps(payload))
            print(r.status_code)
            print(r.text)
        except Exception as e:
            print(str(e))
            print(f'error in {d2}')
        time.sleep(5)


def delete_indices():
    days = [start_date + datetime.timedelta(n) for n in range(int((end_date - start_date).days))]
    for day in days:
        d1 = day.strftime('%Y-%m-%d')
        d2 = day.strftime('%Y.%m.%d')
        repo_path = "tmh_sc_site-{}".format(d2)
        url = 'https://' + host + '/' + repo_path
        try:
            r = requests.delete(url, auth=awsauth, headers=headers, data=json.dumps(payload))
            print(r.status_code)
            print(r.text)
        except Exception as e:
            print(str(e))
        time.sleep(3)


def main():
    restore_snap_index()
    # delete_indices()


if __name__ == '__main__':
    main()
