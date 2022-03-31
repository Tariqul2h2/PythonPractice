import json
from pprint import pprint
import time
import datetime
import curator
import boto3
from elasticsearch.client import CatClient
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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

catClient = CatClient(client)
botoClient = boto3.client('ce')

today = datetime.date.today()
lastMonth = today.replace(day=1) - datetime.timedelta(days=1)


def es_cost_explorer():
    strt = time.time()
    final_result = dict()
    try:
        _date_filter_value = f'{lastMonth.strftime("%Y.%B").lower()}|{lastMonth.strftime("%Y.%m")}'
        il_charging = curator.IndexList(client)
        il_charging.filter_kibana(exclude=True)
        il_charging.filter_closed(exclude=True)
        il_charging.filter_by_regex(kind='regex', value=_date_filter_value)
        il_charging.filter_by_regex(kind='regex', value='_sc_|_sch|device_status')

        il_grid = curator.IndexList(client)
        il_grid.filter_kibana(exclude=True)
        il_grid.filter_closed(exclude=True)
        il_grid.filter_by_regex(kind='regex', value=_date_filter_value)
        il_grid.filter_by_regex(kind='regex', value='_sc_|_sch|device_status', exclude=True)

        print('charging: ', il_charging.indices)
        print('grid: ', il_grid.indices)
        charging_indices_count = len(il_charging.indices)
        charging_indices_size = 0

        grid_indices_count = len(il_grid.indices)
        grid_indices_size = 0

        for index in il_charging.indices:
            res = catClient.indices(index=index, bytes='m', format='json')
            charging_indices_size += int(res[0]['store.size'])

        for index in il_grid.indices:
            res = catClient.indices(index=index, bytes='m', format='json')
            grid_indices_size += int(res[0]['store.size'])

        grid_percentage = round((grid_indices_size / (grid_indices_size + charging_indices_size)) * 100, 2)
        charging_percentage = round((charging_indices_size / (grid_indices_size + charging_indices_size)) * 100, 2)
        es_cost = botoClient.get_cost_and_usage(
            TimePeriod={
                'Start': lastMonth.replace(day=1).strftime('%Y-%m-%d'),
                'End': datetime.date.today().replace(day=1).strftime('%Y-%m-%d')
            },
            Granularity='MONTHLY',
            Filter={
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': [
                        'Amazon OpenSearch Service',
                    ]
                }
            },
            Metrics=[
                'BlendedCost',
            ]
        )['ResultsByTime']
        total_cost = float(es_cost[0]['Total']['BlendedCost']['Amount'])
        grid_cost = total_cost * grid_percentage / 100
        charging_cost = total_cost * charging_percentage / 100
        final_result = {
            'total': {
                'number_of_indices': grid_indices_count + charging_indices_count,
                'accumulated_size_mb': grid_indices_size + charging_indices_size,
                'percentage': 100,
                'cost': round(total_cost, 2)
            },
            'grid': {
                'number_of_indices': grid_indices_count,
                'accumulated_size_mb': grid_indices_size,
                'percentage': grid_percentage,
                'cost': round(grid_cost, 2)
            },
            'charging': {
                'number_of_indices': charging_indices_count,
                'accumulated_size_mb': charging_indices_size,
                'percentage': charging_percentage,
                'cost': round(charging_cost, 2)
            }
        }
        print(f'Elasticsearch cost of {lastMonth.strftime("%B, %Y")}')
        pprint(es_cost)
        pprint(final_result)
    except Exception as e:
        print(str(e))

    print(f'time taken {time.time() - strt} seconds')
    return {
        'statusCode': 200,
        'body': json.dumps(final_result)
    }


if __name__ == '__main__':
    es_cost_explorer()
