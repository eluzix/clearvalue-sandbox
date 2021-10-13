import datetime
import time

import boto3
import yaml

from clearvalue import app_config
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import DBKeys

if __name__ == '__main__':
    dev_profile = boto3.session.Session(profile_name='cvprofile')
    boto3.setup_default_session(profile_name='cvprofile')

    # dynamo = boto3.client('dynamodb')
    # table = ddb.Table('cv-accounts-dev')

    with open('account-data.yaml', 'r') as f:
        all_data = yaml.load(f, yaml.FullLoader)

    today = utils.yesterday()

    batch = []
    for user in all_data['users']:
        uid = user['uid']
        for account in user['accounts']:
            account_id = account['account_id']
            db_account = {
                DBKeys.HASH_KEY: uid,
                DBKeys.SORT_KEY: DBKeys.account_info(account_id),
                'account_id': account_id,
                'uid': uid,

                'created_at': int(time.time()),
                'last_update_at': int(time.time()),

                '30days_return': 0,
                '30days_gain': 0,
                '30days_value': 0,
                '90days_return': 0,
                '90days_gain': 0,
                '90days_value': 0,
                'ytd_return': 0,
                'ytd_gain': 0,
                'ytd_value': 0,
            }

            db_account.update(account)
            batch.append(db_account)

            tp_item = db_account.copy()
            tp_item[DBKeys.HASH_KEY] = account_id
            tp_item[DBKeys.SORT_KEY] = DBKeys.account_time_point(utils.yesterday())
            batch.append(tp_item)

        # for holding in user['holdings']:
        #     db_holding = {
        #         DBKeys.HASH_KEY: holding['account_id'],
        #         DBKeys.SORT_KEY: DBKeys.account_holding(holding['holding_id']),
        #         'account_id': holding['account_id'],
        #         'uid': uid,
        #
        #         'created_at': int(time.time()),
        #         'last_update_at': int(time.time()),
        #     }
        #     db_holding.update(holding)
        #     batch.append(db_holding)

    ddb.batch_write_items(app_config.resource_name('accounts'), batch)

