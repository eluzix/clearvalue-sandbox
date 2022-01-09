import datetime

import boto3

import cvutils
from clearvalue import app_config
from cvcore.store import DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    table_name = app_config.resource_name('accounts')
    uid = 'e28e9038-c962-4e46-960e-c2b77a6468f0'
    account_id = 'eeb21386-0dd0-4973-badb-b2d7dfe41da7'
    cash_value = 256684.88
    cur_date = cvutils.date_from_str('2021-10-25')
    today = cvutils.today()

    account = ddb.get_item(table_name, DBKeys.user_account(uid, account_id))
    account['cash'] = 0

    keys = []
    while cur_date < today:
        keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(cur_date)))
        cur_date += datetime.timedelta(days=1)
        
    tp_data = ddb.batch_get_items(table_name, keys)

    print(f"{tp_data[4]['value']}, {tp_data[4]['cash']}")
    for item in tp_data:
        item['value'] -= item.get('cash', 0)
        item['cash'] = 0
    print(f"{tp_data[4]['value']}, {tp_data[4]['cash']}")

    tp_data.append(account)
    # ddb.batch_write_items(table_name, tp_data)

