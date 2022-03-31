from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd

es = Elasticsearch(host='localhost', port=9200)

def get_data_from_elastic():
    # query: The elasticsearch query.
    query = {
    "size": 1, 
    "query": {
        "match": {
            "customer_full_name": "Eddie Underwood"
            }
        }
    }

    # Scan function to get all the data. 
    rel = scan(client=es,             
               query=query,                                     
               scroll='1m',
               index='ecommerce_data',
               raise_on_error=True,
               preserve_order=False,
               clear_scroll=True)

    # Keep response in a list.
    result = list(rel)
    print(result)
    temp = []

    # We need only '_source', which has all the fields required.
    # This elimantes the elasticsearch metdata like _id, _type, _index.
    # for hit in result:
    #     temp.append(hit['_source'])
        # print(temp)
    # Create a dataframe.
#     df = pd.DataFrame(temp)

#     return df


# df = get_data_from_elastic()

# print(df.head())
if __name__ == '__main__':
    get_data_from_elastic()