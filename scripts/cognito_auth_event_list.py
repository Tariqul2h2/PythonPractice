import boto3
import csv
from pprint import pprint
from datetime import datetime, timezone

client = boto3.client('cognito-idp')
user_pool_id = 'eu-west-1_BSfOuC8hH'
users_auth_logs = [['username', 'auth_time (UTC)', 'Event ID', 'Device']]

kibana_users = client.list_users(
    UserPoolId=user_pool_id
)['Users']
for user in kibana_users:
    username = user.get('Username')
    # print('=========================================================')
    # print(f'working with user: {username}')
    auth_events = client.admin_list_user_auth_events(
        UserPoolId=user_pool_id,
        Username=username
    )['AuthEvents']

    for event in auth_events:
        print(type(event.get('CreationDate').astimezone(timezone.utc), ))
        auth_log = [
            username,
            event.get('CreationDate').astimezone(timezone.utc),
            event.get('EventId'),
            event.get('EventContextData').get('DeviceName')]
        users_auth_logs.append(auth_log)

pprint(users_auth_logs)
print(len(users_auth_logs))

users_auth_logs = sorted(users_auth_logs, key=lambda x: x[1])

file_name = f'kibana_user_auth_log_{datetime.utcnow()}_UTC.csv'
with open(file_name, 'w') as _file:
    _writer = csv.writer(_file)
    _writer.writerows(users_auth_logs)
