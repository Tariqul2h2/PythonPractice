import json
import requests
from sys import argv
import pandas as pd




Index_name = argv[1]
host_name = 'localhost'
port = 9200
host_address = 'localhost:9200'
header = {
    'Content-Type': 'application/json'
}
# es = Elasticsearch(host='localhost', port=9200)

payload = {
  "size": 1000, 
  "query": {
    "match_all": {
    }
  },
  "_source": ["customer_full_name","email","geoip"]
}

scroll_payload = {
    "scroll" : "1m",
    "scroll_id" : None
}
scroll_path = '_search/scroll'
# index_path = Index_name + '/_search?scroll=1m'
index_path = Index_name + '*/_search?scroll=1m'
# index_path = Index_name + '/_search'

url = f'http://{host_address}/{index_path}'
# print(url)

# curl -XGET "http://elasticsearch:9200/ecommerce_data/_search" -H 'Content-Type: application/json'
# curl -XGET "http://elasticsearch:9200/ecommerce_data/_search"
# https://localhost:9200/ecommerce_data/_search
# response = requests.get(url, headers = header, data = json.dumps(payload))
file_count = 0
count = 0
try:
    response = requests.get(url, headers = header, data = json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']
    temp_file = [d['_source'] for d in data]
    df = pd.DataFrame(temp_file)
    file_name = 'files/file_{}.csv'.format(file_count)
    df.to_csv(file_name,index=None)
    print(file_name)
    count = len(data)
    file_count = file_count + 1
    scroll_payload['scroll_id'] = query_response['_scroll_id']
    # print(scroll_payload)
    url = f'http://{host_address}/{scroll_path}'
    # url = f'http://' + host_address + '/' + scroll_path
    # print(len(query_response['hits']['hits']))
    # print(url)
    while len(query_response['hits']['hits']):
        # print(len(query_response['hits']['hits']))
        response = requests.get(url, headers = header, data = json.dumps(scroll_payload))
        query_response = json.loads(response.text)
        data = query_response['hits']['hits']
        temp_data = [d['_source'] for d in data]
        df = pd.DataFrame(temp_data)
        file_name = 'files/file_{}.csv'.format(file_count)
        print(file_name)
        df.to_csv(file_name,index=None)
        file_count = file_count + 1
        scroll_payload['scroll_id'] = query_response['_scroll_id']
        count = count + len(data)
        # count = count + 1
    # print(len(data))
    print(count)
    # print(temp_file)
    # for d in temp_file:
    #     count = count + 1
    #     df = pd.DataFrame(d.get('geoip'))
    #     df.to_csv('datasa.csv',index=None)
    # # print(count)
    # with open('new_file.csv','w') as file:
    #     for d in temp_file:
    #         xx = d.get('geoip')
    #         # df = pd.DataFrame(d.get('geoip'))
    #         # df.to_csv('datasa.csv', index=None)
    #         file.write(str(df))    
        # print(xx)
        # rj = pd.read_json(r'xx')
        # rj.to_csv(r'new_data.csv',index=None)
        # print(rj)
        # df = pd.DataFrame(d.get('geoip'))
        # df = pd.DataFrame(xx)
        # with open('data_new.csv','w') as file:
        #     file.write(str(df))
        # print(df)
        # df.to_csv('datasa.csv', index=None)
        # for z in d['_source']:
            # print(z)
            # xx = z.get('geoip')
            # print(xx)
    # with open('datas.csv','w') as file:
    #     temp_file = [d['_source'] for d in data]
    #     # count = count + 1
    #     # file.write(str(temp_file))
    #     df = pd.DataFrame(temp_file)
    #     df.to_csv('data.csv', index=None)
    #     for d in data:
    #         for z in d['_source']:
    #             xx = z.get('geoip')
    #             print(xx)
    # print(count)
        # print(file)
    # data = query_response['hits']['hits']
    # print(data)
    # print(type(query_response))
    # print(query_response)
    # print(temp_data)
    # df = pd.DataFrame(data, '_source')
    # df.to_csv('data.csv', index=None)
    # print(d2)
    # print(response.status_code)
    # print(response.text)
    # print(Index_name)
except Exception as e:
    print(str(e))
    # print(f'error in {d2}')


# df = pd.read_json (r'Path where the JSON file is saved\File Name.json')
# df.to_csv (r'Path where the new CSV file will be stored\New File Name.csv', index = None)

def to_csv(data,file_name):
    pass
    try:
        doc_count = len(data)
        with open(file_name,'w') as file:
            temp_data = [d['_source'] for d in data]
        print(temp_data)
        df = pd.read_json(data)
        df.to_csv('data.csv', index=None)
        # print(type(data))


    except Exception as e:
        print('Error is ',e)

# def to_csv(data, file_name):
#     try:
#         doc_count = len(data)
#         with open(file_name, 'w') as file:
#             temp_data = [[d['_source'].get('@timestamp'), d['_source'].get('evs'), d['_source'].get('total_power'),
#                           d['_source'].get('total_unmanaged_power')] for d in data]
#             temp_data.insert(0, ['@timestamp', 'evs', 'total_power', 'total_unmanaged_power'])
#             csv_writer = csv.writer(file)
#             csv_writer.writerows(temp_data)
#         print('{} inserted {} data in this batch'.format(file_name, doc_count))
#         return doc_count
#     except Exception as e:
#         print(str(e))
#         raise Exception('Unable to insert data.')
# if __name__ == '__main__':
#     to_csv(data,'new.csv')
