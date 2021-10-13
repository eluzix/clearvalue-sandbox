import boto3

from clearvalue import app_config
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes, TransactionType


def fix_user(uid):
    batch = []
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
    for ac in accounts:
        if ac.get('is_high_level') is True:
            continue

        if ac.get('is_manual') is None or not ac.get('is_manual'):
            continue

        account_id = ac['account_id']
        transactions = loaders.load_account_transactions(account_id)
        for tr in transactions:
            if tr.get('transaction_type') == TransactionType.DIVIDEND.value:
                quantity = tr.get('quantity')
                if quantity is None or quantity == '-':
                    # print(f'REMOVING: {account_id} / {tr}')
                    batch.append(DBKeys.hash_sort(tr[DBKeys.HASH_KEY], tr[DBKeys.SORT_KEY]))

    if len(batch) > 0:
        print(f'For {uid} deleting {len(batch)} transactions')
        ddb.batch_delete_items(app_config.resource_name('accounts'), batch)


if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        fix_user(uid)
