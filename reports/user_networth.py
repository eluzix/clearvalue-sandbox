import datetime
import time

import boto3

from clearvalue import app_config
from clearvalue.graphql import filter_inactive_accounts
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountStatus, AccountTypes

if __name__ == '__main__':

    tp1 = time.time()
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    accounts_table_name = app_config.resource_name('accounts')
    end_date = utils.today()
    days_ago = end_date - datetime.timedelta(days=4)
    start_date = end_date - datetime.timedelta(days=30)
    end_date = utils.date_to_str(end_date)
    start_date = utils.date_to_str(start_date)

    sd = utils.today() - datetime.timedelta(days=120)
    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        created_at = utils.date_from_timestamp(user['created_at'])
        if created_at < sd:
            continue

        account_status = [AccountStatus.ACTIVE.value, AccountStatus.CLOSED.value]
        accounts = loaders.load_user_accounts(uid, load_status=account_status)
        accounts = filter_inactive_accounts(accounts,
                                            '30days',
                                            start_date,
                                            end_date)

        accounts = [a for a in accounts if a['is_manual'] is False and a['account_type'] != AccountTypes.CRYPTO.value]

        total_date_value = 0
        tp_values = ddb.batch_get_items(accounts_table_name,
                                        [DBKeys.hash_sort(a['account_id'], DBKeys.account_time_point(days_ago)) for a in accounts],
                                        projection_expression='HashKey,#value', expression_attribute_names={'#value': 'value'})
        tp_values = {i[DBKeys.HASH_KEY]: i['value'] for i in tp_values}

        for account in accounts:
            current_val = float(account['value'])
            days_ago_val = tp_values.get(account['account_id'], 0)

            if current_val - days_ago_val > 1000000:
                print(f'[{uid}] {account["account_id"]} ({account["account_type"]}): {current_val} - {days_ago_val} = {current_val - days_ago_val}')

            # account_type = account['account_type']
            # property_id = None
            # if account_type == AccountTypes.LOAN.value:
            #     property_id = account.get('property_id')
            #
            # if account_type == AccountTypes.LOAN.value:
            #     if property_id is None:
            #         total_date_value -= float(account['value'])
            # else:
            #     total_date_value += float(account['value'])



        # if total_date_value > 5000000:
        #     print(f'{uid} ({created_at}) == {total_date_value}')

    tp2 = time.time()
    print(f'All done in {tp2-tp1}')
    # print(f'Networth: {total_date_value} done in {tp2 - tp1}')
