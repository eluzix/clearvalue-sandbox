import datetime

import boto3

from clearvalue import app_config
from clearvalue.analytics import get_active_config, query_cursor, is_user_active, is_internal_user
from clearvalue.analytics.segmentation import config_segment
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.search import elastic
from clearvalue.lib.store import DBKeys


def load_active_logins(boto_session, for_date: datetime.datetime = None):
    if for_date is None:
        for_date = utils.today()

    start_date = for_date - datetime.timedelta(days=30)

    client = elastic.client(boto_session=boto_session)
    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                {"match": {"category": "user"}},
                {"match": {"event": "login"}}
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "gte": utils.date_to_str(start_date),
                        "lte": utils.date_to_str(for_date),
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }

    login_count = {}
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
        source = hit['_source']
        uid = source.get('uid')
        if is_internal_user(uid):
            continue

        count = login_count.get(uid, 0)
        login_count[uid] = count + 1

    keys = [{
        DBKeys.HASH_KEY: u,
        DBKeys.SORT_KEY: f'STATS'
    } for u in login_count.keys()]

    users = ddb.batch_get_items(app_config.resource_name('analytics'), keys)
    active_users = [user for user in users if is_user_active(user)]

    login_slots = {}
    for user in active_users:
        uid = user[DBKeys.HASH_KEY]
        count = login_count.get(uid)
        count_stats = login_slots.get(count, [])
        count_stats.append(uid)
        login_slots[count] = count_stats

    slots_keys = list(login_slots.keys())
    slots_keys.sort()

    data = [(sk, len(login_slots[sk])) for sk in slots_keys]
    for logins, users in data:
        print(f'[{logins},{users}],')


if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # for_date = utils.today()
    load_active_logins(boto_session)
    # active_config = get_active_config()
    # segment = config_segment(active_config, run_for=for_date)
    # uids = [u[DBKeys.HASH_KEY] for u in segment]

    # print(len(uids))
