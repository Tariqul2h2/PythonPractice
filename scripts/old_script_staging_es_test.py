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
# INDEX_INCLUDE_PREFIX = 'tmh_sc_site|tmh_sc_statistics_20|tmh_sc_billing'
INDEX_INCLUDE_PREFIX = 'cloned_tmh_sc_site'

# prod
host = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'
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
    ilo.filter_by_regex(kind='regex', value=INDEX_INCLUDE_PREFIX)
    ilo.filter_by_regex(kind='timestring', value=last_month.strftime(DATE_FORMAT_INDEX))

    if event['type'] == 'merge': # this part is done
        unique_index_names = list(set([index.split(f'-{last_month.year}')[0] for index in ilo.indices]))
        for idx in unique_index_names:
            src = f'{idx}-{last_month.year}.{last_month.month:02}*'
            dst = f'{idx}-{last_month.year}.{calendar.month_name[last_month.month].lower()}'
            tsk = f'merging {src} into {dst}'
            try:
                print(tsk)
                client.reindex({
                    "source": {"index": src},
                    "dest": {"index": dst}
                }, wait_for_completion=False, timeout='120m')
                time.sleep(10)
            except Exception as e:
                print(e)
                # this exception is raised to ask Lambda function to retry
                raise Exception(f'failed {tsk}')

    elif event['type'] == 'remove':
        unique_index_names = list(set([index.split(f'-{last_month.year}')[0] for index in ilo.indices]))
        for idx in unique_index_names:
            src = f'{idx}-{last_month.year}.{last_month.month:02}*'
            dst = f'{idx}-{last_month.year}.{calendar.month_name[last_month.month].lower()}'
            try:
                src_count = client.count(index=src)
                dst_count = client.count(index=dst)
                if src_count['count'] == dst_count['count']:
                    client.indices.delete(index=src)
                    print(f'Daily indices of {src} are deleted.')
                else:
                    raise Exception(f'document count of {src} and {dst} are not same.')
            except Exception as e:
                print(str(e))
                print(f'Failed deleting {src}.')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


lambda_handler(event={'type': 'remove'}, context=None)

