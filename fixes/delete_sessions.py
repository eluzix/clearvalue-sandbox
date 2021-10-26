import logging

import boto3

from clearvalue import app_config
from clearvalue.lib.store import loaders, DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    for user in loaders.iter_users():
        batch = []
        uid = user[DBKeys.HASH_KEY]
        all_sessions = ddb.query(app_config.resource_name('analytics'),
                                 KeyConditionExpression='HashKey = :HashKey AND begins_with(SortKey, :SortKey)',
                                 ExpressionAttributeValues={
                                     ':HashKey': ddb.serialize_value(uid),
                                     ':SortKey': ddb.serialize_value('UID:SESSION:')
                                 })
        for s in all_sessions:
            batch.append({DBKeys.HASH_KEY: s[DBKeys.HASH_KEY], DBKeys.SORT_KEY: s[DBKeys.SORT_KEY]})

        if len(batch) > 0:
            print(f'Deleting {len(batch)} records for {uid}')
            ddb.batch_delete_items(app_config.resource_name('analytics'), batch)

    print('All done')

