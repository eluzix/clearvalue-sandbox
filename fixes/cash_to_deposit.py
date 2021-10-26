import boto3

from clearvalue import app_config
from cvcore.store import loaders
from cvcore.model.cv_types import AccountTypes, AccountStatus
from cvutils import cognito_utils
from cvutils.dynamodb import ddb


def cash_fix(uid):
    batch = []
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.CASH, load_active_only=False)
    for a in accounts:
        status = a.get('account_status', AccountStatus.ACTIVE.value)
        if status == AccountStatus.DELETED.value:
            continue

        subtype = a.get('account_subtype')
        if subtype == 'deposit':
            a['account_type'] = AccountTypes.DEPOSIT.value
            batch.append(a)

    if len(batch) > 0:
        print('for {} updating {} accounts'.format(uid, len(batch)))
        ddb.batch_write_items(app_config.resource_name('accounts'), batch)
    else:
        print('user {} has no deposits'.format(uid))


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')
    # business_fix('79bea3db-b638-4ec7-9c74-7fdebb1afae2')

    for user in cognito_utils.iterate_users():
        for att in user['Attributes']:
            if att['Name'] == 'sub':
                uid = att['Value']
                cash_fix(uid)
