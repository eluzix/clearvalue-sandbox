import csv
import time

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.analytics import is_internal_user, is_user_active, get_active_config
from cvutils.store.keys import DBKeys
from cvcore.store import loaders


def active_users(run_for=None, active_group=1):
    tp1 = time.time()
    if run_for is None:
        stats_date = utils.date_to_str(utils.today())
    else:
        stats_date = run_for

    active_group_config = get_active_config(active_group)
    rows = []
    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        if is_internal_user(uid):
            continue

        if run_for is None:
            daily_stats = ddb.get_item(app_config.resource_name('analytics'), DBKeys.hash_sort(uid, 'STATS'))
        else:
            daily_stats = ddb.get_item(app_config.resource_name('analytics'), DBKeys.hash_sort(uid, f'STATS:{run_for}'))

        if daily_stats is None:
            continue

        if not is_user_active(daily_stats, active_group_config):
            continue

        rows.append([uid, str(daily_stats['user_age'])])

    with open(f'active_users_age_{stats_date}.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['UID', 'User Age'])
        writer.writerows(rows)

    print('All Done')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    active_users(run_for='2021-09-14')
