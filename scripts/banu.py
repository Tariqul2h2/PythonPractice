<<<<<<< Updated upstream
# DOS-209
import os
=======
import os
import curator
import json
import sys
>>>>>>> Stashed changes
import csv
import boto3
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from pprint import pprint

# authentication
region = 'eu-west-1'
service = 'es'

# # staging
# host = 'vpc-staging-mobilityhouse-qzusvj2sfzjyviij4dpv5xa4ma.eu-west-1.es.amazonaws.com'
# prod
host = 'vpc-the-mobilityhouse-dpxinpje6ppvrbithyefgxwq7a.eu-west-1.es.amazonaws.com'
# index_name = 'tmh_sc_site-2020*'
rl = f'https://{host}'
headers = {"Content-Type": "application/json"}

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def main():
    # create folder to store result files
    try:
        if not os.path.exists('banu'):
            os.makedirs('banu')
    except OSError as e:
        print(str(e))
        print('unable to create folder')
        raise Exception('Unable to create folder "files" to store results.')
    indices = [
<<<<<<< Updated upstream
        'tmh_sc_site-2020*',
        'tmh_sc_billing-2020*',
        'tmh_sc_total_and_fleet-2020.*',
=======
        'tmh_sc_site-2020.10.*',
        'tmh_sc_billing-2020.10.*',
        'tmh_sc_total_and_fleet-2020.10.*',
>>>>>>> Stashed changes
        'lc_configuration*'
    ]
    for index_name in indices:
        file_name = f"banu/{index_name.split('-')[0]}.csv"
        url = f'https://{host}/_cat/indices/{index_name}?v&h=index,docs.count,tm&bytes=k&s=index:asc'
        response = requests.get(url, auth=awsauth, headers=headers)
        with open(file_name, 'w') as file:
            writer = csv.writer(file)
            for line in response.text.splitlines():
                writer.writerow(line.split())


if __name__ == '__main__':
    main()
<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
