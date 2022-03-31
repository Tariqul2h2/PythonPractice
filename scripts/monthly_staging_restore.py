import curator
import json
import datetime
import time
from elasticsearch import Elasticsearch
import boto3
import requests
from requests_aws4auth import AWS4Auth
from calendar import monthrange

############### custom input starts ###############
YEAR = 2021
# month options [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
MONTH = 12

INDEX_NAME = "*"
CHARGE_INDEX_TYPE = "newprod-charging-snapshots-repo"
GRID_INDEX_TYPE = "newprod-grid-snapshots-repo"
############### custom input ends ###############

num_days = monthrange(YEAR, MONTH)[1]
region = 'eu-west-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
# staging
staging_host = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'

headers = {"Content-Type": "application/json"}
payload = {
    "indices": ""
}


def staging_restore_snap_index():
    for day in range(1, num_days + 1):
        _date = datetime.datetime(year=YEAR, month=MONTH, day=day)
        d1 = _date.strftime('%Y-%m-%d')
        d2 = _date.strftime('%Y.%m.%d')

        for INDEX_TYPE in [GRID_INDEX_TYPE, CHARGE_INDEX_TYPE]:
            repo_path = f"_snapshot/{INDEX_TYPE}/snapshot_of__{d1}/_restore"
            payload = {
                "indices": f"{INDEX_NAME}-{d2}",
                "rename_pattern": "(.+)",
                "rename_replacement": "script_testing_$1"
            }
            url = f"https://{staging_host}/{repo_path}"
            try:
                r = requests.post(url, auth=awsauth, headers=headers, data=json.dumps(payload))
                print(d2)
                print(r.status_code)
                print(r.text)
                print(INDEX_NAME)
            except Exception as e:
                print(str(e))
                print(f'error in {d2}')

            time.sleep(20)


def staging_delete_indices():
    for day in range(1, num_days + 1):
        _date = datetime.datetime(year=YEAR, month=MONTH, day=day)
        # d1 = _date.strftime('%Y-%m-%d')
        d2 = _date.strftime('%Y.%m.%d')
        repo_path = f"restored_{INDEX_NAME}-{d2}"
        url = f"https://{staging_host}/{repo_path}"
        try:
            r = requests.delete(url, auth=awsauth, headers=headers, data=json.dumps(payload))
            print(r.status_code)
            print(r.text)
        except Exception as e:
            print(str(e))
        time.sleep(10)


def main():
    staging_restore_snap_index()
    # staging_delete_indices()


if __name__ == '__main__':
    main()

