import json
import boto3
import requests
from datetime import datetime, timedelta
import time
# from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# authentication
region = 'eu-west-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'vpc-the-mobilityhouse-dpxinpje6ppvrbithyefgxwq7a.eu-west-1.es.amazonaws.com'
yesterday = datetime.today() - timedelta(days=1)
index_name = f'device_status-{yesterday.strftime("%Y.%m.%d")}'
headers = {"Content-Type": "application/json"}
scroll_path = '_search/scroll'
payload = {
    "size": 10000,
    "query": {
        "match_all": {}
    },
    "_source": ["@timestamp", "chargers"]
}
charger_payload = {
    "size": 10000,
    "query": {
        "bool": {
            "must": [
                {
                    "match_all": {}
                }
            ],
            "filter": {
                "range": {
                    "@timestamp": {
                        "gte": "now-31d/d",  # yesterday -30 days
                        "lte": "now-1d/d"  # yesterday
                    }
                }
            }
        }
    },
    "_source": ["@timestamp", "chargers"]
}
scroll_payload = {
    "scroll": "1m",
    "scroll_id": None
}

charger_status = {}
unique_chargers_list = set()


def get_unique_chargers(data):
    try:
        doc_count = len(data)
        for doc in data:
            tmp = doc.get('_source')
            chargers = tmp.get('chargers')
            if chargers:
                for charger in chargers:
                    unique_chargers_list.add(charger['id'])
                    # print(charger['id'])
        return doc_count
        # process here
    except Exception as e:
        print(str(e))
        raise Exception('Unable to process data.')


def get_charger_beats_timestamp(data):
    try:
        doc_count = len(data)
        for doc in data:
            tmp = doc.get('_source')
            timestamp = datetime.strptime(tmp['@timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
            chargers = tmp.get('chargers')
            for charger in chargers:
                if charger['status'] not in ["offline", "error"]:
                    try:
                        charger_status[charger['id']].append(timestamp)
                    except Exception as e:
                        print(str(e))
                        charger_status[charger['id']] = [timestamp, ]

        return doc_count
        # process here
    except Exception as e:
        print(str(e))
        raise Exception('Unable to process data.')


def config_charger_status():
    unique_chargers = list(unique_chargers_list)
    for charger in unique_chargers:
        charger_status[charger] = []


def charger_id_extractor():
    total_count = 0
    # part 1: get first 10000 data and scroll_id
    index_path = 'device_status-*' + '/_search?scroll=1m'
    url = 'https://' + host + '/' + index_path
    # set device_id
    response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(charger_payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']  # extracting the first page
    move_count = get_unique_chargers(data)
    total_count = total_count + move_count

    # Step 2: Request to scroll with the scroll_id
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    url = 'https://' + host + '/' + scroll_path
    while len(query_response['hits']['hits']):
        response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']  # extracting the next 10000 data
        move_count = get_unique_chargers(data)
        total_count = total_count + move_count
        scroll_payload['scroll_id'] = query_response['_scroll_id']


def charger_data_extractor():
    total_count = 0
    # part 1: get first 10000 data and scroll_id
    index_path = index_name + '/_search?scroll=1m'
    url = 'https://' + host + '/' + index_path
    # set device_id
    response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']  # extracting the first page
    move_count = get_charger_beats_timestamp(data)
    total_count = total_count + move_count

    # Step 2: Request to scroll with the scroll_id
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    url = 'https://' + host + '/' + scroll_path
    while len(query_response['hits']['hits']):
        response = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']  # extracting the next 10000 data
        move_count = get_charger_beats_timestamp(data)
        total_count = total_count + move_count
        scroll_payload['scroll_id'] = query_response['_scroll_id']


def outage_calculator():
    for charger_id, beat_times in charger_status.items():
        sorted_beat_times = sorted(beat_times)
        if sorted_beat_times:  # beats available in last 24 hours
            first_beat = sorted_beat_times[0]
            last_beat = sorted_beat_times[-1]
        else:  # no beats available in last 24 hours
            first_beat = last_beat = yesterday

        if not (first_beat.hour == 0 and first_beat.minute == 0):
            day_start = first_beat.replace(hour=0, minute=0, second=0, microsecond=0)
            sorted_beat_times.insert(0, day_start)
        if not (last_beat.hour == 23 and last_beat.minute == 59):
            day_end = last_beat.replace(hour=23, minute=59, second=59, microsecond=59)
            sorted_beat_times.append(day_end)

        outage_count = 0
        outage_time_in_seconds = 0
        loop_range = len(sorted_beat_times)  # to get the last time

        for indx in range(1, loop_range):
            try:
                time_diff = (sorted_beat_times[indx] - sorted_beat_times[indx - 1]).seconds
                if time_diff >= 900:
                    outage_count += 1
                    outage_time_in_seconds += time_diff
                    # print('from {} to {}, time diff: {}'.format(sorted_beat_times[indx - 1], sorted_beat_times[indx], time_diff))
            except Exception as e:
                print(e)
                print(indx, loop_range, len(sorted_beat_times))

        percentage = round((float(outage_time_in_seconds) / float(60 * 60 * 24)) * 100, 4)
        print(f'{yesterday.strftime("%Y-%m-%d"):12} : {charger_id:20} : {outage_count:5} : {outage_time_in_seconds:8} : {percentage:5}')


def main():
    start = time.time()
    charger_id_extractor()
    config_charger_status()
    charger_data_extractor()
    print('Date: charger_id: outage_count: outage_time_in_seconds: percentage')
    outage_calculator()
    # with open('charger_status_2.json', 'w') as file:
    #     json.dump(charger_status, file, indent=2, default=str)
    print(f'total time taken: {time.time() - start} seconds')


if __name__ == '__main__':
    main()

