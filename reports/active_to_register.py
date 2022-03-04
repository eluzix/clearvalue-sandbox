import boto3

import cvutils
from clearvalue import app_config
from clearvalue.analytics import query_cursor, is_user_active, ACTIVE_GROUPS
from cvcore.store import DBKeys
from cvutils import elastic
from cvutils.dynamodb import ddb


def generate_stats(start_date, end_date=None, active_for=None, boto_session=None):
    if end_date is None:
        end_date = cvutils.date_to_str(cvutils.now())

    client = elastic.client(boto_session=boto_session)
    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                {"match": {"category": "signup"}},
                {"match": {"event": "'user created'"}}
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "gte": start_date,
                        "lte": end_date,
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }
    all_ids = set()
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
        source = hit['_source']
        # doc_date = datetime.datetime.strptime(source['ts'], '%Y-%m-%dT%H:%M:%S.%f%z')
        uid = source['uid']
        all_ids.add(uid)

    if active_for is None:
        sort_key = 'STATS'
    else:
        sort_key = f'STATS:{active_for}'

    db_keys = [DBKeys.hash_sort(uid, sort_key) for uid in all_ids]
    users = ddb.batch_get_items(app_config.resource_name('analytics'), db_keys)
    results = {}
    for user in users:
        for group in ACTIVE_GROUPS:
            if is_user_active(user, group):
                group_stats = results.get(group['name'], 0)
                results[group['name']] = group_stats + 1

    all_joins = len(all_ids)
    print(f'All Joins between {start_date} and {end_date} are {all_joins}')
    for group in ACTIVE_GROUPS:
        group_stats = results.get(group['name'], 0)
        print(f'For {group["name"]} total users {group_stats}, conversion rate is {(group_stats / all_joins) * 100:.2f}%')


if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    start_date = '2021-08-22'
    # end_date = cvutils.date_to_str(cvutils.today())
    # end_date = None
    # end_date = '2022-02-22'
    end_date = '2021-11-22'
    generate_stats(start_date, end_date=end_date, active_for='2021-12-22', boto_session=boto_session)
