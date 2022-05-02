import csv

import boto3

from clearvalue import app_config
from clearvalue.lib.import_processors import process_crypto_transactions
from cvcore.store import DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')

    with open('/Users/uzix/Downloads/crypto-transactions.csv', 'r') as fin:
        reader = csv.reader(fin)
        reader.__next__()
        uid = 'befba013-635c-4662-a298-ebe163c3f50c'
        account_id = '1a0e3e9f-5b74-4387-912b-8fe60b0b83ee'
        account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
        process_crypto_transactions(reader, uid, account)
