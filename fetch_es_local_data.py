from calendar import monthrange
from sys import argv, exit

# year = int(argv[1]) #year number
# month = int(argv[2]) #month number
# Index_name = argv[3]
index_name = argv[1]

# time_stamp1 = argv[2]
# time_stamp2 = argv[3]

payload = {
    "size": 10, 
  "query": {
    "match": {
      "customer_full_name": "Eddie Underwood"
    }
  }
}
headers = {"Content-Type": "application/json"}



# num_days = monthrange(year,month)[1] #find total days
