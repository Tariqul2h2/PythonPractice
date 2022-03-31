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
YEAR = 2020
# month options [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
#<<<<<<< Updated upstream
#MONTH = 1
#INDEX_NAME = "tmh_sc_billing"
#INDEX_TYPE = "charging-snapshots-repo"
#INDEX_TYPE = "grid-snapshots-repo"
#=======
MONTH = 12
INDEX_NAME = "tmh_sc_site"
INDEX_TYPE = "charging-snapshots-repo"
#INDEX_TYPE = "grid-snapshots-repo"
#>>>>>>> Stashed changes
############### custom input ends ###############

num_days = monthrange(YEAR, MONTH)[1]
region = 'eu-west-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
# prod
prod_host = 'vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'

headers = {"Content-Type": "application/json"}
payload = {
    "indices": ""
}


def prod_restore_snap_index():
    for day in range(1, num_days + 1):
        _date = datetime.datetime(year=YEAR, month=MONTH, day=day)
        d1 = _date.strftime('%Y-%m-%d')
        d2 = _date.strftime('%Y.%m.%d')
        repo_path = f"_snapshot/{INDEX_TYPE}/snapshot_of__{d1}/_restore"
        payload = {
            "indices": f"{INDEX_NAME}-{d2}"
        }
        url = f"https://{prod_host}/{repo_path}"
        try:
            r = requests.post(url, auth=awsauth, headers=headers, data=json.dumps(payload))
            print(d2)
            print(r.status_code)
            print(r.text)
        except Exception as e:
            print(str(e))
            print(f'error in {d2}')
#<<<<<<< Updated upstream
        time.sleep(20)
#=======
     ##   time.sleep(10)
#>>>>>>> Stashed changes


def main():
    prod_restore_snap_index()


if __name__ == '__main__':
    main()
#<<<<<<< Updated upstream
#=======

#>>>>>>> Stashed changes
