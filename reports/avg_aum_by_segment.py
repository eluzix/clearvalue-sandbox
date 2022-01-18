import csv
import json
import time

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.analytics import segment_for_aum, is_internal_user, is_user_active, get_active_config, ACTIVE_GROUPS
from cvcore.model.cv_types import AccountTypes
from cvcore.store import loaders
from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb


def user_db_stats(uid, for_date=None):
    accounts = loaders.load_user_accounts(uid, load_active_only=False)
    asset_types = set()
    liability_types = set()
    user_net_worth = 0
    user_linked_net_worth = 0
    total_assets = 0
    total_liabilities = 0

    if for_date is None:
        tp_values = {}
    else:
        tp_values = ddb.batch_get_items(app_config.resource_name('accounts'), [DBKeys.hash_sort(a['account_id'], DBKeys.account_time_point(for_date)) for a in accounts],
                                        projection_expression='HashKey,#value,market_value', expression_attribute_names={'#value': 'value'})
        tp_values = {i[DBKeys.HASH_KEY]: i['value'] for i in tp_values}

    for account in accounts:
        account_id = account['account_id']
        account_status = account.get('account_status')
        account_value = float(tp_values.get(account_id, account['value']))
        account_type = account['account_type']
        if account_type == AccountTypes.REAL_ESTATE.value:
            market_value = tp_values.get('market_value', account.get('market_value'))
            if market_value is not None:
                account_value = float(market_value)

        # account_value = float(account['value'])
        if account_value > 100000000:
            print(f'[_users_stats] ignoring {uid} account for AUM {account_value}')
            continue

        if account_status in ['active', 'closed']:
            if account_type == AccountTypes.LOAN.value:
                # liability_types.add(account['account_subtype'])
                total_liabilities += abs(account_value)
                user_net_worth -= account_value
            else:
                # asset_types.add(account_type)
                total_assets += abs(account_value)
                user_net_worth += account_value

            if account.get('is_manual') is False:
                if account_type == AccountTypes.LOAN.value:
                    user_linked_net_worth -= account_value
                else:
                    user_linked_net_worth += account_value

    aum = float(total_assets + total_liabilities)
    stats = {
        'aum': aum,
        'net_worth': float(user_net_worth),
        'aum_segment': segment_for_aum(aum),
        'net_worth_segment': segment_for_aum(user_net_worth),
        'linked_aum': float(user_linked_net_worth),
        'linked_aum_segment': segment_for_aum(user_linked_net_worth),
        'assets': total_assets,
        'liabilities': total_liabilities,
    }

    return stats


# def all_users():
#     tp1 = time.time()
#     today = utils.today()
#     buckets = {}
#     linked_buckets = {}
#     for user in loaders.iter_users():
#         uid = user[DBKeys.HASH_KEY]
#         if is_internal_user(uid):
#             continue
#
#         us = user_db_stats(uid, today)
#         if us['aum'] == 0:
#             continue
#
#         bucket_info = buckets.get(us['aum_segment'])
#         if bucket_info is None:
#             bucket_info = [0, 0]
#         bucket_info[0] += 1
#         bucket_info[1] += us['aum']
#         buckets[us['aum_segment']] = bucket_info
#
#         bucket_info = linked_buckets.get(us['linked_aum_segment'])
#         if bucket_info is None:
#             bucket_info = [0, 0]
#         bucket_info[0] += 1
#         bucket_info[1] += us['aum']
#         linked_buckets[us['linked_aum_segment']] = bucket_info
#
#     with open('avg_aum_by_segment.json', 'w') as fout:
#         json.dump({'segments': buckets, 'linked_segments': linked_buckets}, fout)
#
#     tp2 = time.time()
#     print(f'All done in {tp2 - tp1}')


def active_users(run_for=None, active_group=0, all_users=None, stats_map=None):
    tp1 = time.time()
    if run_for is None:
        stats_date = utils.today()
    else:
        stats_date = utils.date_from_str(run_for)

    if all_users is None or stats_map is None:
        all_users = []
        db_keys = []
        for user in loaders.iter_users():
            uid = user[DBKeys.HASH_KEY]
            if is_internal_user(uid):
                continue

            all_users.append(user)

            if run_for is None:
                db_keys.append(DBKeys.hash_sort(uid, 'STATS'))
            else:
                db_keys.append(DBKeys.hash_sort(uid, f'STATS:{run_for}'))
        db_data = ddb.batch_get_items(app_config.resource_name('analytics'), db_keys)
        stats_map = {stats[DBKeys.HASH_KEY]: stats for stats in db_data}

    today = utils.today()
    buckets = {}
    linked_buckets = {}
    all_aum = []
    for user in all_users:
        uid = user[DBKeys.HASH_KEY]
        if is_internal_user(uid):
            continue

        daily_stats = stats_map.get(uid)

        if daily_stats is None:
            continue

        if not is_user_active(daily_stats, get_active_config(active_group)):
            continue

        us = user_db_stats(uid, stats_date)
        if us['aum'] == 0:
            continue

        all_aum.append(us['aum'])

        bucket_info = buckets.get(us['aum_segment'])
        if bucket_info is None:
            bucket_info = [0, 0]
        bucket_info[0] += 1
        bucket_info[1] += us['aum']
        buckets[us['aum_segment']] = bucket_info

        bucket_info = linked_buckets.get(us['linked_aum_segment'])
        if bucket_info is None:
            bucket_info = [0, 0]
        bucket_info[0] += 1
        bucket_info[1] += us['aum']
        linked_buckets[us['linked_aum_segment']] = bucket_info

    with open(f'avg_aum_by_segment_{active_group}.json', 'w') as fout:
        json.dump({'segments': buckets, 'linked_segments': linked_buckets, 'all_aum': all_aum}, fout)

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')

    return all_users, stats_map


def print_results(active_group):
    with open(f'avg_aum_by_segment_{active_group}.json', 'r') as fin:
        js = json.load(fin)

    keys = list(js['segments'].keys())
    keys.sort()
    for k in keys:
        if k == '1':
            segment = '0-$500k'
        elif k == '2':
            segment = '$500k-$1m'
        elif k == '3':
            segment = '$1m-$5m'
        elif k in ['4', '0']:
            segment = '$5m+'
        else:
            raise Exception(f'unknown segment {k}')
        print(f'for segment {segment} avg. AUM is ${int(js["segments"][k][1] / js["segments"][k][0])}')


def debug_aum(active_group, run_for=None, all_users=None, stats_map=None):
    tp1 = time.time()
    if run_for is None:
        stats_date = utils.today()
    else:
        stats_date = utils.date_from_str(run_for)

    if all_users is None or stats_map is None:
        all_users = []
        db_keys = []
        for user in loaders.iter_users():
            uid = user[DBKeys.HASH_KEY]
            if is_internal_user(uid):
                continue

            all_users.append(user)

            if run_for is None:
                db_keys.append(DBKeys.hash_sort(uid, 'STATS'))
            else:
                db_keys.append(DBKeys.hash_sort(uid, f'STATS:{run_for}'))
        db_data = ddb.batch_get_items(app_config.resource_name('analytics'), db_keys)
        stats_map = {stats[DBKeys.HASH_KEY]: stats for stats in db_data}

    with open('aum_debug.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['AUM', 'Net Worth', 'Assets', 'Liabilities'])
        for user in all_users:
            uid = user[DBKeys.HASH_KEY]
            if is_internal_user(uid):
                continue

            daily_stats = stats_map.get(uid)

            if daily_stats is None:
                continue

            if not is_user_active(daily_stats, get_active_config(active_group)):
                continue

            us = user_db_stats(uid, stats_date)
            if us['aum'] == 0:
                continue

            writer.writerow([str(us['aum']), str(us['net_worth']), str(us['assets']), str(us['liabilities'])])

    print('all done')
    return all_users, stats_map


if __name__ == '__main__':
    # profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # all_users()
    # active_users('2021-11-11', 0)
    all_users = None
    stats_map = None
    # all_users, stats_map = debug_aum(0, '2021-11-11', all_users=all_users, stats_map=stats_map)

    i = 3
    all_users, stats_map = active_users('2022-01-15', i, all_users=all_users, stats_map=stats_map)
    print_results(i)

    # for i, group in enumerate(ACTIVE_GROUPS):
    #     print(f"Processing data for group {group['desc']}")
    #     all_users, stats_map = active_users('2021-11-11', i, all_users=all_users, stats_map=stats_map)
    #     print_results(i)
