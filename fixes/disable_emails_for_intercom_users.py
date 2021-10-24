import csv

import boto3

from clearvalue import app_config
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import DBKeys

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    table_name = app_config.resource_name('accounts')
    UID_COL = 3

    with open('/Users/uzix/Downloads/Claritus_Users_351156_export_2021-10-24_06_59.csv', 'r') as fin:
        reader = csv.reader(fin)
        # strip headers
        reader.__next__()
        uids = set()
        for row in reader:
            if row[UID_COL] != '':
                uids.add(row[UID_COL])

    keys = [DBKeys.info_key(uid) for uid in uids]
    users = ddb.batch_get_items(table_name, keys)
    batch = []
    for user in users:
        batch.append({
            DBKeys.HASH_KEY: user[DBKeys.HASH_KEY],
            DBKeys.SORT_KEY: DBKeys.NOTIFICATION_SETTINGS,
            'weekly_summary': False,
            'monthly_report': False,
            'new_features': False,
            'activity_reports': False,
        })

    ddb.batch_write_items(table_name, batch)

