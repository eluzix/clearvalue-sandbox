import time

import boto3

from clearvalue import app_config
from cvanalytics import iter_active_users
from cvcore.store import DBKeys
from cvutils import elastic
from cvutils.dynamodb import ddb


def fix_user_history(uid, boto_session):
    table_name = app_config.resource_name('analytics')
    tp_items = ddb.query(table_name,
                         KeyConditionExpression='HashKey = :HASH AND begins_with(SortKey, :SortKey)',
                         ExpressionAttributeValues={
                             ':HASH': ddb.serialize_value(uid),
                             ':SortKey': ddb.serialize_value('STATS:'),
                         })
    for item in tp_items:
        item.update({
            DBKeys.GS1_HASH: 'USER_STATS_HISTORY',
            DBKeys.GS1_SORT: f'{item["SortKey"]}:{uid}'
        })

    print(f'For {uid} writing {len(tp_items)} items')
    ddb.batch_write_items(table_name, tp_items)

    for item in tp_items:
        item['id'] = f"us-{item[DBKeys.HASH_KEY]}-{item[DBKeys.SORT_KEY]}"
        remove_keys = []
        for k in item:
            if item[k] == '-':
                remove_keys.append(k)
        for k in remove_keys:
            del item[k]
    elastic.index_docs('users-stats', tp_items, boto_session=boto_session)
    

if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # fix_user_history('001199ae-9d73-49e9-8ca6-68d64117903e', boto_session)
    tp1 = time.time()
    for user in iter_active_users(load_latest=True, active_only=False):
        fix_user_history(user['uid'], boto_session)

    tp2 = time.time()
    print(f'Done in {tp2-tp1}')