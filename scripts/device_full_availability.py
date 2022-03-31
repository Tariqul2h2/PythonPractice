import json
import requests
from datetime import datetime, timedelta
from pprint import pprint
from math import sqrt

# import sys
# import boto3
# import curator
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth

# authentication
region = 'eu-west-1'
service = 'es'

host = 'vpc-the-mobilityhouse-dpxinpje6ppvrbithyefgxwq7a.eu-west-1.es.amazonaws.com'  # prod
# host = 'vpc-staging-mobilityhouse-qzusvj2sfzjyviij4dpv5xa4ma.eu-west-1.es.amazonaws.com'  # staging
headers = {"Content-Type": "application/json"}

query = {
    "size": 0,
    "query": {
        "range": {
            "@timestamp": {
                "gte": "now-1d",
                "lte": "now-1d"
            }
        }
    },
    "aggs": {
        "device": {
            "terms": {"field": "device_id.keyword", "size": 10000}
        }
    }
}


def check_percentage(check_date):
    # set date for checking
    query['query']['range']['@timestamp']['gte'] = check_date
    query['query']['range']['@timestamp']['lte'] = check_date

    index_name = 'device_status-*'
    url = 'https://{}/{}/_search'.format(host, index_name)
    all_data = requests.get(url, headers=headers, data=json.dumps(query))

    json_data = json.loads(all_data.text)
    # pprint(json_data)
    buckets = json_data['aggregations']['device']['buckets']
    fully_available = [device for device in buckets if device['doc_count'] >= 1440]
    total_devices = len(buckets)
    solid = len(fully_available)

    percentage = 0
    if total_devices:
        percentage = solid / total_devices

    # print('check_date: {}, total_devices: {}, heartbeat >= 1440: {}, percentage: {:.2%}'.format(
    #     check_date, total_devices, solid, percentage))
    return [check_date, total_devices, solid, round(percentage, 4)]


def main():
    """
    provide only start_date if you only want result of one day i.e. 2020.08.24,
    provide both start_date and end_date for a range query. By default, that is set for yesterday.
    """
    start_date = '2020-08-23'  # 2020-08-24 , default, now-1d
    end_date = '2020-08-31'  # 2020-08-24, default, now-1d
    # start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    # end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    result_list = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end = datetime.strptime(start_date, "%Y-%m-%d")

    """
    Calculate 3 Standard Deviation:
    1. Get percentage of 30 days from search date range's last date, i.e. x1,x2,x3...x30
    2. get mean of 30 days.xm = (x1+x2+x3+...+x30)/30
    3. calculate variance of the set. xv = ((x1-xm)**2)+(x2-xm)**2)+...+(x30-xm)**2))/30
    4. 1 Standard Deviation, SD = sqrt(xv)
    5. 3 Standard Deviation, 3SD = SD*3
    6. 3SD upper/lower = xm + 3SD / xm - 3SD  
    """
    list_30 = []
    last_30_days = [end - timedelta(days=x) for x in range(30, 0, -1)]
    for day in last_30_days:
        result = check_percentage(day.strftime("%Y-%m-%d"))
        list_30.append(result[3])
    mean = sum(list_30) / 30
    variance = sum([(elem - mean) ** 2 for elem in list_30]) / 30
    sd1 = sqrt(variance)
    high_3sd = round((mean + (sd1 * 3)) * 100, 2)
    low_3sd = round((mean - (sd1 * 3)) * 100, 2)

    # result for date range from search
    date_list = [start + timedelta(days=x) for x in range(0, (end - start).days + 1)]

    for day in date_list:
        result = check_percentage(day.strftime("%Y-%m-%d"))
        result_list.append(result+[high_3sd, low_3sd])

    data = [
        {
            "columns": [
                {"text": "Date", "type": "time"},
                {"text": "Total LC", "type": "number"},
                {"text": "LC (Heartbeat>=1440)", "type": "number"},
                {"text": "Percentage(%)", "type": "number"},
                {"text": "μ + 3σ", "type": "number"},
                {"text": "μ - 3σ", "type": "number"}
            ],
            "rows": result_list,
            "type": "table"
        }
    ]

    pprint(data)


if __name__ == '__main__':
    main()
