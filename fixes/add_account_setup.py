import json
import time

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import DataProvider
from cvutils import lambda_utils
from cvutils.dynamodb import ddb


def fix_user(user):
    if user.get('yodlee_id') is None:
        return

    tp1 = time.time()
    uid = user[DBKeys.HASH_KEY]
    data = lambda_utils.invoke({'uid': uid, 'norm': False}, 'yodlee.prod_sandbox')
    data = json.loads(data)
    y_accounts = data['accounts']
    if len(y_accounts) == 0:
        return

    table_name = app_config.resource_name('accounts')
    accounts_to_load = [(DataProvider.YODLEE, ac['id']) for ac in y_accounts]
    db_accounts = loaders.load_provider_items(accounts_to_load, uid)
    for account in y_accounts:
        provider = DataProvider.YODLEE
        created_date = account.get('createdDate')
        if created_date is None:
            continue
        created_date = utils.timestamp_from_string(created_date.split('T')[0])

        db_account = db_accounts.get('{}:{}'.format(provider.value, account['id']))
        if db_account is not None:
            # print(f"for {uid} -- {db_account['account_id']} setting {created_date}")
            ddb.update_with_fields(table_name, DBKeys.hash_sort(db_account[DBKeys.HASH_KEY], db_account[DBKeys.SORT_KEY]), {'account_setup_at': created_date}, ['account_setup_at'])
    tp2 = time.time()
    print(f'For {uid} done in {tp2-tp1} for {len(db_accounts)} accounts')


    # accounts = loaders.load_user_accounts(uid)
    # linked_accounts = []
    # for account in accounts:
    #     if account['is_manual'] is False and account['account_type'] != AccountTypes.CRYPTO.value:
    #         linked_accounts.append(account)
    #
    # if len(linked_accounts) > 0:
    #     data = lambda_utils.invoke({'uid': uid, 'norm': False}, 'yodlee.prod_sandbox')
    #     data = json.loads(data)
    #     for y_account in data['accounts']:
    #         pass

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    tp1 = time.time()
    for user in loaders.iter_users():
        fix_user(user)
    tp2 = time.time()
    print(f'All done in {tp2-tp1}')

    # user = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key('2bb40134-1a88-4491-bedf-496401a429f0'))
    # fix_user(user)
