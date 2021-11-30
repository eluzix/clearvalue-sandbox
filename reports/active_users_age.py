import csv
import time

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.analytics import is_internal_user, is_user_active, get_active_config, ACTIVE_GROUPS
from cvcore.store import loaders
from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb


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

    with open(f'active_users_age_{stats_date}_{active_group}.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['UID', 'User Age'])
        writer.writerows(rows)

    analyze(stats_date, active_group)

    print('All Done')


def analyze(stats_date, active_group):
    per_format = lambda v: f'{(v * 100):.2f}%'

    with open(f'active_users_age_{stats_date}_{active_group}.csv', 'r') as fin:
        reader = csv.reader(fin)
        reader.__next__()
        data = {}
        total = 0
        for user in reader:
            total += 1
            age = int(user[1])
            if age <= 90:
                group = '1-3'
            elif age <= 180:
                group = '3-6'
            elif age <= 270:
                group = '6-9'
            else:
                group = '9+'

            group_data = data.get(group, 0)
            data[group] = group_data + 1

        keys = list(data.keys())
        keys.sort()
        for group in keys:
            group_data = data[group]
            print(f'For group {group} data is {per_format(group_data/total)}')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    for i, group in enumerate(ACTIVE_GROUPS):
        print(f"Processing data for group {group['name']}")
        active_users(run_for='2021-11-11', active_group=i)
        print('-------------------------------')
    # analyze(stats_date='2021-11-11')
