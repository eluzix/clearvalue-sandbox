import csv

import boto3

from clearvalue import app_config
from clearvalue.analytics import is_internal_user, get_active_config, is_user_active, iter_active_users
from cvcore.store import loaders, DBKeys
from cvutils.dynamodb import ddb


def active_by_assets():
    keys = []
    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        if is_internal_user(uid):
            continue
        keys.append(DBKeys.hash_sort(uid, 'STATS'))

    users_stats = ddb.batch_get_items(app_config.resource_name('analytics'), keys)
    active_group_config = get_active_config(3)
    meta_keys = []
    for user in users_stats:
        if is_user_active(user, active_group_config):
            meta_keys.append(DBKeys.hash_sort(user[DBKeys.HASH_KEY], 'META_STATS'))

    users_meta = ddb.batch_get_items(app_config.resource_name('analytics'), meta_keys)
    stats = {}
    for meta in users_meta:
        first_session_create_count = meta.get('first_session_create_count', 0)
        if first_session_create_count == 0:
            key = 'no assets'
        elif first_session_create_count < 4:
            key = '1-3 assets'
        else:
            key = '4+ assets'

        type_count = stats.get(key, 0)
        type_count += 1
        stats[key] = type_count
    print(stats)


def all_users_assets():
    users = {}
    meta_keys = []
    for user in iter_active_users(load_latest=True, active_only=False):
        uid = user[DBKeys.HASH_KEY]
        users[uid] = user
        meta_keys.append(DBKeys.hash_sort(uid, 'META_STATS'))
    users_meta = ddb.batch_get_items(app_config.resource_name('analytics'), meta_keys)
    with open('all_users_by_assets.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['User', 'Assets Count', 'First Session Assets Count', 'Total Sessions', 'Campaign', 'Keyword'])
        for user in users_meta:
            uid = user[DBKeys.HASH_KEY]
            user_info = users[uid]
            total_active_accounts = str(user_info.get('total_active_accounts', 0))
            total_sessions = str(user_info.get('total_sessions', 0))
            first_session_create_count = str(user.get('first_session_create_count', 0))
            utm_campaign = user.get('utm_campaign', '')
            utm_term = user.get('utm_term', '')
            writer.writerow([uid, total_active_accounts, first_session_create_count, total_sessions, utm_campaign, utm_term])

    print('All Done')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # active_by_assets()
    all_users_assets()