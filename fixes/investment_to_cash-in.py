import boto3

from clearvalue import app_config
from clearvalue.lib import cognito_utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders
from clearvalue.model.cv_types import AccountTypes, AccountStatus


def business_fix(uid):
    batch = []
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.BUSINESS, load_active_only=False)
    for a in accounts:
        status = a.get('account_status', AccountStatus.ACTIVE.value)
        if status == AccountStatus.DELETED.value:
            continue

        transactions = loaders.load_account_transactions(a['account_id'])
        for t in transactions:
            if t['transaction_type'] == 'investment':
                print('For uid {}/{} found investment transaction'.format(uid, a['account_id']))
                t['transaction_type'] = 'cash-in'
                batch.append(t)

    if len(batch) > 0:
        print('for {} updating {} transaction'.format(uid, len(batch)))
        ddb.batch_write_items(app_config.resource_name('accounts'), batch)


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')
    # business_fix('79bea3db-b638-4ec7-9c74-7fdebb1afae2')

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        business_fix(uid)
