import json
import datetime as dt
import logging
import time
import boto3
import curator
import calendar
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logging.basicConfig(level=logging.INFO)
DATE_FORMAT_INDEX = '%Y.%m'
INDEX_INCLUDE_PREFIX = 'tmh_sc|device_status'

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


def lambda_handler(event, context):
    # get the last date of last month
    last_month = dt.datetime.today().replace(day=1) - dt.timedelta(days=1)
    ilo = curator.IndexList(client)
    # filter out kibana related indices
    ilo.filter_kibana(exclude=True)
    # only get indices that starts with either `tmh_sc` or `device_status`, those are the SC indices
#    ilo.filter_by_regex(kind='regex', value=INDEX_INCLUDE_PREFIX)
    # as we have issue with `tmh_sc_site`, skip those indices
#    ilo.filter_by_regex(kind='regex', value='tmh_sc_site', exclude=True)
    ilo.filter_by_regex(kind='timestring', value=last_month.strftime(DATE_FORMAT_INDEX))
    unique_index_names = list(set([index.split(f'-{last_month.year}')[0] for index in ilo.indices]))

    print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n=============================================================')
    print(unique_index_names)
    for n in unique_index_names:
        print(n)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

lambda_handler(None, None)
