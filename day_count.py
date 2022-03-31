from calendar import monthrange
import datetime
from wsgiref import headers
from wsgiref.simple_server import server_version
from numpy import indices
import requests
import boto3
import json
from requests_aws4auth import AWS4Auth

from sys import argv
year = int(argv[1]) #year number
month = int(argv[2]) #month number
Index_name = argv[3]


num_days = monthrange(year,month)[1] #find total days

# Index_name = 'tmh_sc_billing'
if(argv[4] == 'charging'):
    Index_repo = 'charging-snapshots-repo'
elif(argv[4] == 'grid'):
    Index_repo = 'grid-snapshots-repo'

headers = {
    'Content-Type' : 'application/json'
}

payload = {
    'indices' : '',
    'rename_pattern' : '(.+)',
    'rename_replacement': "restored_$1"
}

print(f'Year: {year}\nMonth:{month}\nIndex_name:{Index_name}\nRepo_name:{Index_repo}')

# Index_repo = 'charging-snapshots-repo'
region = 'eu-west-1'
service = 'es'
cred = boto3.Session().get_credentials()

# awsauth = AWS4Auth(cred.access_key, cred.secret_key, region, service, session_token = cred.token)

host_address = 'vpc-the-mobilityhouse-es-6br6u2rkmj5eepsdlo56bvkuxe.eu-west-1.es.amazonaws.com'






# print(num_days)
def xfer_restore():
    for day in range(1,num_days + 1):
        _date = datetime.datetime(year=year, month=month, day=day)
        # print(_date)
        d1 = _date.strftime('%Y-%m-%d')
        d2 = _date.strftime('%Y.%m.%d')
        # print(d1)
        # print(d2)
        repo_path = f'_snapshot/{Index_repo}/snapshot_of__{d1}/_restore'
        print(repo_path)
        # POST /_snapshot/snapshots-charging/snapshot_of__2021-12-01/_restore
        payload = {
            'indices' : f'{Index_name}-{d2}',
            'rename_pattern' : '(.+)',
            'rename_replacement': "restored_$1"
        }
        url = f'https://{host_address}/{repo_path}'
        print(url)
        # try:
        #     r = requests.post(url,auth=awsauth, headers=headers, data = json.dumps(payload))
        # except Exception as e:
        #     print(e)

