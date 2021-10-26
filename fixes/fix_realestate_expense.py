import boto3

from clearvalue import app_config
from clearvalue.graphql.schema import loans
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes
from cvutils.dynamodb import ddb


def fix_user_accounts(uid):
    table_name = app_config.resource_name('accounts')
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.REAL_ESTATE)
    for account in accounts:
        loan_id = account.get('loan_id')
        account_id = account['account_id']
        if loan_id is not None:
            print('fixing account {} for uid {}'.format(account_id, uid))
            loan = ddb.get_item(table_name, DBKeys.user_account(uid, loan_id))
            update_item = {}
            update_fields = []
            loans._handle_expenses(update_item, account, loan['loan_recurring_payment'], update_fields)
            ddb.update_with_fields(table_name,
                                   DBKeys.user_account(uid, account_id),
                                   update_item, update_fields)


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # fix_user_accounts('4aaa981b-004b-4c39-a743-979ee062ddee')

    # for user in cognito_utils.iterate_users():
    #     uid = cognito_utils.uid_from_user(user)
    #     fix_user_accounts(uid)
