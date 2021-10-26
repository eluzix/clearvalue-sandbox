import boto3

from clearvalue import app_config
from cvcore.calcs import run_account_calcs
from cvutils.store.keys import DBKeys
from cvcore.store import loaders
from cvcore.model.cv_types import AccountTypes
from cvutils.dynamodb import ddb


def check_user_loans(uid):
    accounts_table_name = app_config.resource_name('accounts')

    user_accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.LOAN)
    for loan in user_accounts:
        if loan.get('is_manual') is True:
            property_id = loan.get('property_id')
            if property_id is not None:
                # realestate_account = ddb.get_item(accounts_table_name, DBKeys.user_account(uid, property_id))
                realestate_account = ddb.get_item(accounts_table_name, DBKeys.hash_sort(property_id, DBKeys.account_time_point('2021-07-19')))
                print(f'>>>> loan value: {loan["value"]} == on property: {realestate_account["loan_value"]}')


def fix_user_loans(uid):
    accounts_table_name = app_config.resource_name('accounts')

    user_accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.LOAN)
    for loan in user_accounts:
        if loan.get('is_manual') is True:

            if loan.get('currency', 'USD') != 'USD':
                continue

            if 'uid' not in loan:
                print(f'UID not found for {uid} / {loan["account_id"]}')
                ddb.update_with_fields(accounts_table_name, DBKeys.user_account(uid, loan['account_id']), {'uid': uid}, ['uid'])

            print(f'Running for {uid} / {loan["account_id"]}')
            run_account_calcs(uid, loan['account_id'], 'loan-history', update_today=True)


if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        fix_user_loans(uid)

    # uid = '4aaa981b-004b-4c39-a743-979ee062ddee'
    # fix_user_loans(uid)
