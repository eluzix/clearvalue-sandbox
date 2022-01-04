import datetime
import time

import boto3
import pytz

import cvutils
from clearvalue import app_config
from cvcore.calcs.portfolio import update_account_holdings_latest_data
from cvcore.store import DBKeys, loaders
from cvutils.config import get_app_config
from cvutils.dynamodb import ddb


def day_of_interest(dt: datetime.datetime, day_of_interest: int) -> int:
    first_of_month = datetime.datetime(dt.year, dt.month, 1, tzinfo=pytz.utc)
    interest_date = first_of_month + datetime.timedelta(days=day_of_interest - 1)

    if dt.month == interest_date.month:
        return day_of_interest

    # if interest day isn't in the month return last day of month
    last_of_month = interest_date - datetime.timedelta(days=interest_date.day)
    return last_of_month.day


def w():
    all_errors = {}
    error_type = 'action:{action_name}'
    existing_error = all_errors.get(error_type)
    if existing_error is None:
        all_errors[error_type] = 'only 1 error message here'
    else:
        all_errors[error_type] = 'general error message here'


def timing_test1():
    uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'
    account_id = 'ff12268d-1815-40d7-9e7f-617a2c14cf41'
    account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    start_date = '2020-10-21'
    end_date = cvutils.date_to_str(cvutils.today())

    tp1 = time.time()
    graph_data = loaders.load_account_graph_data(account, 'custom', start_date, end_date)
    tp2 = time.time()
    print(f'[TEST 1] Loaded {len(graph_data)} in {tp2 - tp1}')


def timing_test2():
    uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'
    account_id = 'ff12268d-1815-40d7-9e7f-617a2c14cf41'
    account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    start_date = '2020-10-21'
    end_date = cvutils.date_to_str(cvutils.today())

    tp1 = time.time()
    cur_date = cvutils.date_from_str(start_date) + datetime.timedelta(days=1)
    keys = [DBKeys.hash_sort(account_id, DBKeys.account_time_point(start_date))]
    run_till = cvutils.date_from_str(end_date)
    while cur_date < run_till:
        if cur_date.weekday() == 6:
            keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(cur_date)))
        # keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(cur_date)))
        cur_date = cur_date + datetime.timedelta(days=1)
    keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(end_date)))

    # graph_data = loaders.load_account_graph_data(account, 'custom', start_date, end_date)
    graph_data = ddb.batch_get_items(app_config.resource_name('accounts'), keys)

    tp2 = time.time()
    print(f'[TEST 2] Loaded {len(graph_data)} in {tp2 - tp1}')


def query():
    accounts_table_name = get_app_config().resource_name('accounts')
    account_id = '4df35a2c-0186-497d-972b-3b1a045fad8a'
    start_date = '2000-01-01'
    end_date = '2000-01-03'

    if isinstance(start_date, str):
        start_date = cvutils.date_from_str(start_date)
    if isinstance(end_date, str):
        end_date = cvutils.date_from_str(end_date)

    items = ddb.query(accounts_table_name,
                      KeyConditionExpression='HashKey = :HashKey AND SortKey BETWEEN :start_date AND :end_date',
                      ExpressionAttributeValues={
                          ':HashKey': ddb.serialize_value(account_id),
                          ':start_date': ddb.serialize_value(DBKeys.account_time_point(start_date)),
                          ':end_date': ddb.serialize_value(DBKeys.account_time_point(end_date)),
                      })
    print(items)


if __name__ == '__main__':
    # boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    # timing_test1()
    # timing_test2()
    # print(ddb.batch_get_items(app_config.resource_name('accounts'), [DBKeys.user_account('sd', '12')]))
    # query()

    db_account = loaders.load_user_account('5bbc7a83-fa1a-42d3-a908-b5cebb9a7e09', '1d8dc5aa-8c64-4c3d-99d4-0624d9fe61da')
    print(db_account['value'])
    update_account_holdings_latest_data(db_account)
    print(db_account['value'])

    # ddb.collect_query_data = True
    # ret = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key('f417d7b5-cb66-4ef1-a36a-9c6806d0af0f'))
    # print(ret)
