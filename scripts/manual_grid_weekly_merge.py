import boto3
import curator
import calendar
from datetime import date, timedelta, datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

SNAPSHOT_OFFSET = 1  # which days snapshot will be taken
DATE_FORMAT_INDEX = '%Y.%m.%d'
DATE_FORMAT_SNAPSHOT = '%Y-%m-%d'  # ES snapshot name can't contain `.` char
WEEK_FORMAT_INDEX = '%Y.%W'
MONTH_FORMAT_INDEX = '%Y.%m'
daily_config_list = [('tmh_controller_metric', 366),
                     ('tmh_relay', 3)]  # rule for delete older indices of specific indices

# staging
#host = 'vpc-new-staging-mobilityhouse-xhcg36ixdejfvboetkt2gi6qs4.eu-west-1.es.amazonaws.com'

# # prod
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

script_execution_day = datetime.today()

def get_pre_date(query='date'):
    """
    arg: query = year or month or week_number
    Return previous year or month or week number
    """
    today = script_execution_day

    if query == 'month':
        this_month = today.month
        if int(this_month) == 1:
            previous_month = 12
        else:
            previous_month = int(this_month) - 1
        if len(str(previous_month)) > 1:
            return str(previous_month)
        else:
            return '0' + str(previous_month)
    elif query == 'year':
        this_year = today.year
        this_month = today.month
        if int(this_month) == 1:
            return str(int(this_year) - 1)
        return str(this_year)
    elif query == 'week_number':
        this_week = today.isocalendar()[1]  # iso week number
        if this_week == 1:
            previous_week = 52
        else:
            previous_week = int(this_week) - 1
        if len(str(previous_week)) > 1:
            return str(previous_week)
        else:
            return '0' + str(previous_week)
    return today


def all_grid_indices():
    """
    return all grid indices
    """
    ilo = curator.IndexList(client)  # Getting all indices from Client ES

    # Filter all Grid indices
    # Filter all rolling indices
    ilo.filter_by_regex(kind='suffix', value='[\.][0-3][0-9]', exclude=False)
    # Exclude sch indices
    ilo.filter_by_regex(kind='regex', value='sch', exclude=True)
    # Exclude tmh_sc or device_status indices
    ilo.filter_by_regex(kind='prefix', value='(tmh_sc|device_status)', exclude=True)
    return ilo


def weekly_event(event):
    """
    Create reindex using last week indices of tmh_controller_metric
    tmh_controller_metric-yyyy-mm-dd --> tmh_controller_metric-yyyy-week_number
    Execution time: GMT 1.15
    """
    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value='tmh_controller_metric',
                        exclude=False)  # Filter all tmh_controller_metric indices

    today = script_execution_day
    this_month = today.month
    this_day = today.day

    if len(str(this_month)) == 1:
        this_month = '0' + str(this_month)

    if len(str(this_day)) == 1:
        this_day = '0' + str(this_day)

    if len(str(this_month)) == 1:
        this_month = '0' + str(this_month)
    # make today's index name
    today_index = 'tmh_controller_metric-{}.{}.{}'.format(today.year, this_month, this_day)
    # exclude today's index
    ilo.filter_by_regex(kind='prefix', value=today_index, exclude=True)
    # Filter last 7 days indices
    ilo.filter_by_age(source='name', direction='younger', timestring=DATE_FORMAT_INDEX, unit='days',
                      unit_count=8)
    # Make new index name
    new_index_name = 'tmh_controller_metric-{}.{}'.format(today.year, get_pre_date('week_number'))

    if event['dryrun'] == True or event['dryrun'] is None:
        print('New index name:', new_index_name)
        print("tmh_controller_metric last week indices list:")
        for i in ilo.indices:
            print(i)
        print()
    else:
        print('New index name:', new_index_name)
        print("tmh_controller_metric last week indices list:")
        for i in ilo.indices:
            print(i)
        try:
            client.reindex({
                "source": {"index": ilo.indices},
                "dest": {"index": new_index_name}
            }, wait_for_completion=False, timeout='50m')
        except Exception as e:
            print(str(e))
            raise Exception('Unable to reindex ', new_index_name)


def main():
    global script_execution_day
    script_execution_day = datetime(year=2020, month=12, day=14)
    weekly_event({"dryrun": False})


if __name__ == '__main__':
    main()

