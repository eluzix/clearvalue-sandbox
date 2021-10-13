import csv

import boto3

from clearvalue import app_config
from clearvalue.lib import cognito_utils
from clearvalue.lib.store import loaders
from clearvalue.model.cv_types import AccountStatus

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    with open('users-assets-count.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['uid', 'email', 'name', 'created_at', 'status', 'active_accounts', 'closed_accounts', 'deleted_accounts', 'assets_types'])
        for user in cognito_utils.iterate_users():
            uid = None
            name = None
            email = None
            created_at = user['UserCreateDate']
            status = user['UserStatus']
            for att in user['Attributes']:
                if att['Name'] == 'sub':
                    uid = att['Value']
                elif att['Name'] == 'name':
                    name = att['Value']
                elif att['Name'] == 'email':
                    email = att['Value']

            row = [uid, email, name, created_at.strftime('%c'), status]

            accounts = loaders.load_user_accounts(uid, load_active_only=False)
            active_accounts = 0
            closed_accounts = 0
            deleted_accounts = 0
            account_types = set()
            for a in accounts:
                status = a.get('account_status', AccountStatus.ACTIVE.value)
                if status == AccountStatus.DELETED.value:
                    deleted_accounts += 1
                elif status == AccountStatus.CLOSED.value:
                    closed_accounts += 1
                else:
                    active_accounts += 1

                account_type = a['account_type']
                if account_type == 'loan':
                    account_type = a.get('account_subtype', account_type)
                account_types.add(account_type)

            row.append(active_accounts)
            row.append(closed_accounts)
            row.append(deleted_accounts)
            row.append(len(account_types))

            writer.writerow(row)

    print('*** All Done ***')
