import time

import boto3

from clearvalue import app_config
from clearvalue.lib import utils, cognito_utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes


def fix_user_accounts(uid):
    table_name = app_config.resource_name('accounts')
    accounts = loaders.load_user_accounts(uid, load_active_only=True)
    for a in accounts:
        if bool(a.get('is_manual', False)):
            last_update_at = None
            account_type = a['account_type']
            if account_type == AccountTypes.CRYPTO.value:
                last_update_at = utils.now()
            elif 'rate' in a:
                last_update_at = 1609459200
            elif a.get('is_high_level') is True:
                transactions = ddb.query(table_name,
                                         follow_lek=False,
                                         KeyConditionExpression='HashKey = :HashKey AND begins_with(SortKey, :SortKey)',
                                         ExpressionAttributeValues={
                                             ':HashKey': ddb.serialize_value(a['account_id']),
                                             ':SortKey': ddb.serialize_value('AC:TR:')
                                         },
                                         ScanIndexForward=False,
                                         Limit=1)
                if len(transactions) > 0:
                    tr_date = transactions[0]['transaction_date']
                    last_update_at = utils.timestamp_from_date(utils.date_from_str(tr_date))

            if last_update_at is not None:
                print(f'for {uid}::{a["account_id"]} updating {account_type} to {last_update_at}')
                ddb.update_with_fields(table_name, DBKeys.hash_sort(a[DBKeys.HASH_KEY], a[DBKeys.SORT_KEY]),
                                       {'last_update_at': last_update_at}, ['last_update_at'])


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    # fix_user_accounts('79bea3db-b638-4ec7-9c74-7fdebb1afae2')
    tp1 = time.time()
    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        if uid is not None:
            fix_user_accounts(uid)
    tp2 = time.time()
    print(f'All done in {tp2-tp1}')
