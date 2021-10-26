import json
import time

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.analytics import segment_for_aum, is_internal_user, is_user_active, get_active_config
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes
from cvutils.dynamodb import ddb


def user_db_stats(uid, for_date=None):
    accounts = loaders.load_user_accounts(uid, load_active_only=False)
    asset_types = set()
    liability_types = set()
    user_net_worth = 0
    user_linked_net_worth = 0

    if for_date is None:
        tp_values = {}
    else:
        tp_values = ddb.batch_get_items(app_config.resource_name('accounts'), [DBKeys.hash_sort(a['account_id'], DBKeys.account_time_point(for_date)) for a in accounts],
                                        projection_expression='HashKey,#value', expression_attribute_names={'#value': 'value'})
        tp_values = {i[DBKeys.HASH_KEY]: i['value'] for i in tp_values}

    for account in accounts:
        account_id = account['account_id']
        account_status = account.get('account_status')
        account_value = float(tp_values.get(account_id, account['value']))
        # account_value = float(account['value'])
        if account_value > 100000000:
            print(f'[_users_stats] ignoring {uid} account for AUM {account_value}')
            continue

        if account_status in ['active', 'closed']:
            account_type = account['account_type']
            if account_type == AccountTypes.LOAN.value:
                liability_types.add(account['account_subtype'])
                user_net_worth -= account_value
            else:
                asset_types.add(account_type)
                user_net_worth += account_value

            if account.get('is_manual') is False:
                if account_type == AccountTypes.LOAN.value:
                    user_linked_net_worth -= account_value
                else:
                    user_linked_net_worth += account_value

    stats = {
        'aum': float(user_net_worth),
        'aum_segment': segment_for_aum(user_net_worth),
        'linked_aum': float(user_linked_net_worth),
        'linked_aum_segment': segment_for_aum(user_linked_net_worth),
    }

    return stats


def all_users():
    tp1 = time.time()
    today = utils.today()
    buckets = {}
    linked_buckets = {}
    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        if is_internal_user(uid):
            continue

        us = user_db_stats(uid, today)
        if us['aum'] == 0:
            continue

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

    with open('avg_aum_by_segment.json', 'w') as fout:
        json.dump({'segments': buckets, 'linked_segments': linked_buckets}, fout)

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def active_users(run_for=None):
    tp1 = time.time()
    if run_for is None:
        stats_date = utils.today()
    else:
        stats_date = utils.date_from_str(run_for)

    today = utils.today()
    buckets = {}
    linked_buckets = {}
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

        if not is_user_active(daily_stats, get_active_config(2)):
            continue

        us = user_db_stats(uid, stats_date)
        if us['aum'] == 0:
            continue

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

    with open('avg_aum_by_segment.json', 'w') as fout:
        json.dump({'segments': buckets, 'linked_segments': linked_buckets}, fout)

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def print_results():
    with open('avg_aum_by_segment.json', 'r') as fin:
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
        elif k == '4':
            segment = '$5m+'
        else:
            raise Exception(f'unknown segment {k}')
        print(f'for segment {segment} avg. AUM is ${int(js["segments"][k][1]/js["segments"][k][0])}')


if __name__ == '__main__':
    # profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # all_users()
    active_users('2021-09-14')
    print_results()
