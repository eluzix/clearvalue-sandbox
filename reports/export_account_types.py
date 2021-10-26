import csv
import time

import boto3

from clearvalue import app_config
from cvutils.store.keys import DBKeys
from cvcore.store import loaders
from cvcore.model.cv_types import AccountStatus, AccountTypes

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    tp1 = time.time()

    account_status = [AccountStatus.ACTIVE.value, AccountStatus.CLOSED.value]
    with open('other_accounts.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['name', 'description', 'value segment'])
        for user in loaders.iter_users():
            uid = user[DBKeys.HASH_KEY]
            accounts = loaders.load_user_accounts(uid, load_status=account_status, account_type=AccountTypes.OTHER)
            for account in accounts:
                row = [account['name'], account.get('description', '')]
                val = float(account.get('value', 0))
                if val <= 50000:
                    row.append('0-$50k')
                elif val <= 100000:
                    row.append('$50k-$100k')
                elif val <= 250000:
                    row.append('$100k-$250k')
                elif val <= 500000:
                    row.append('$250k-$500k')
                elif val <= 1000000:
                    row.append('$500k-$1m')
                else:
                    row.append('$1m+')
                writer.writerow(row)

    tp2 = time.time()
    print(f'All done in {tp2-tp1}')
