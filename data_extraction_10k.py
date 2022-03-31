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
  "size": 100, 
  "query": {
    "match_all": {

    }
  },
  "_source": ["customer_full_name","email","geoip"]
}
index_path = Index_name + '/_search'
url = f'http://{host_address}/{index_path}'
file_name = 'extracted_data.csv'
try:
    response = requests.get(url, headers = header, data = json.dumps(payload))
    query_response = json.loads(response.text)
    data = query_response['hits']['hits']
    temp_file = [d['_source'] for d in data]
    df = pd.DataFrame(temp_file)
    df.to_csv(file_name,index=None)

except Exception as e:
    print(str(e))

    