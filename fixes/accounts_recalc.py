import json

import boto3

from clearvalue import app_config
from clearvalue.lib import utils, boto3_client
from clearvalue.lib.calcs import rerun_account_calcs
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes, AccountStatus


def vc_fix(uid):
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.VC, load_active_only=False)
    for a in accounts:
        status = a.get('account_status', AccountStatus.ACTIVE.value)
        if status == AccountStatus.DELETED.value:
            continue
        run_from = utils.date_from_timestamp(a['created_at'])
        run_from = utils.date_to_str(run_from)
        print('for {} updating {} from {}'.format(uid, a['account_id'], run_from))
        rerun_account_calcs(uid, a['account_id'], run_from)


def all_accounts_fix(uid):
    accounts = loaders.load_user_accounts(uid, load_active_only=False)
    for a in accounts:
        status = a.get('account_status', AccountStatus.ACTIVE.value)
        if status == AccountStatus.DELETED.value:
            continue
        run_from = utils.date_from_timestamp(a['created_at'])
        run_from = utils.date_to_str(run_from)
        print('for {} updating {} from {}'.format(uid, a['account_id'], run_from))
        rerun_account_calcs(uid, a['account_id'], run_from)


def account_fix(uid, account_id):
    a = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    status = a.get('account_status', AccountStatus.ACTIVE.value)
    if status == AccountStatus.DELETED.value:
        print('Account {}:{} is deleted'.format(uid, account_id))
        return

    run_from = utils.date_from_timestamp(a['created_at'])
    run_from = utils.date_to_str(run_from)
    print('for {} updating {} from {}'.format(uid, a['account_id'], run_from))
    rerun_account_calcs(uid, a['account_id'], run_from)


def detailed_manual_sp_accounts(uid):
    queue_url = app_config['sqs']['account.calcs.url']
    sqs = boto3_client('sqs')

    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
    for a in accounts:
        if a.get('is_manual') is True and a.get('is_high_level', False) is False:
            account_id = a.get('account_id')
            if account_id is None:
                continue
            print(f'Calling {uid} / {account_id}')
            msg = {'uid': uid, 'account_id': account_id, 'action': 'manual-sp-recalc'}
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))


def detailed_crypto_accounts(uid):
    queue_url = app_config['sqs']['account.calcs.url']
    sqs = boto3_client('sqs')

    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.CRYPTO)
    for a in accounts:
        account_id = a.get('account_id')
        if account_id is None:
            continue

        print(f'Calling {uid} / {account_id}')
        msg = {'uid': uid, 'account_id': account_id, 'action': 'crypto-recalc'}
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))


if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # vc_fix('79bea3db-b638-4ec7-9c74-7fdebb1afae2')

    # all_accounts_fix('2023f23a-9ceb-442e-9c66-0488f8d1c781')

    # account_fix('5e5f0619-7306-46e7-85be-b521ee79f5ad', '6d1dab0e-2a9d-4689-9e9a-69bd1c1520ab')

    # rerun_account_calcs('2023f23a-9ceb-442e-9c66-0488f8d1c781', '6b07e050-7923-421c-80db-5587477cc028', '2019-10-31')

    # for user in cognito_utils.iterate_users():
    #     uid = cognito_utils.uid_from_user(user)
    #     if uid is not None:
    #         vc_fix(uid)

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        detailed_crypto_accounts(uid)
    # detailed_crypto_accounts('b062003d-408f-4a60-8795-7f83081be5be')
