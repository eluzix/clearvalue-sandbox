import concurrent.futures
import datetime
import time

import boto3

import cvutils
from clearvalue import app_config
from clearvalue.analytics import query_cursor
from cvcore.store import DBKeys, loaders
from cvutils import elastic
from cvutils.dynamodb import ddb


def process_user(user, boto_session):
    uid = user[DBKeys.HASH_KEY]

    name = user.get('name')
    if name is None:
        name = f"{user.get('given_name', '')} {user.get('family_name', '')}"

    meta_stats = {
        DBKeys.HASH_KEY: uid,
        DBKeys.SORT_KEY: f'META_STATS',
        DBKeys.GS1_HASH: 'META_STATS',
        DBKeys.GS1_SORT: uid,
        'uid': uid,
        'name': name,
        'created_at': user['created_at'],
        # 'first_session_create_count': create_count,
    }
    if 'join_ip' in user:
        meta_stats['join_ip'] = user['join_ip']

    first_session = ddb.query(app_config.resource_name('analytics'),
                              follow_lek=False,
                              KeyConditionExpression='HashKey = :HashKey AND begins_with(SortKey, :SortKey)',
                              ExpressionAttributeValues={
                                  ':HashKey': ddb.serialize_value(uid),
                                  ':SortKey': ddb.serialize_value('UID:SESSION:')
                              }, Limit=1)
    create_count = 0
    if first_session is not None and len(first_session) > 0:
        first_session = first_session[0]
        start = cvutils.date_from_timestamp(first_session['start']) - datetime.timedelta(seconds=5)
        end = cvutils.date_from_timestamp(first_session['end']) + datetime.timedelta(seconds=5)
        query = {
            'bool': {
                "must": [
                    {"match": {"log_name": "client-log"}},
                    {"match": {"event": "create"}},
                    {"match": {"label": "save"}},
                    {"match": {"uid": uid}},
                ],
                'filter': [{
                    "range": {
                        "ts": {
                            "gte": cvutils.date_to_str(start),
                            "lte": cvutils.date_to_str(end),
                            'format': 'yyyy-MM-dd'
                        }
                    }
                }],
            }
        }
        client = elastic.client(boto_session=boto_session)

        for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
            create_count += 1
    meta_stats['first_session_create_count'] = create_count

    return meta_stats


def add_utm(users, boto_session):
    client = elastic.client(boto_session=boto_session)

    body = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                {"match": {"event": "user created"}},
                {"match": {"category": "signup"}},
            ],
        },
    }
    users_map = {u['uid']: u for u in users}
    for doc in query_cursor(client, body, index='app-logs*'):
        source = doc['_source']
        user = users_map.get(source['uid'])
        if user is None:
            continue

        for field in source:
            field = field.lower()
            if 'utm_' in field or field in ['pains', 'assets']:
                user[field] = source[field]

    return users


if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    tp1 = time.time()
    # uid = '2d4670f6-2bac-4135-a9ac-17d58c60cf4e'
    # user = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key(uid))
    # meta_stats = process_user(user, boto_session)

    batch = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_users = {
            executor.submit(process_user, user, boto_session): user
            for user in loaders.iter_users()
        }

        for future in concurrent.futures.as_completed(future_to_users):
            user = future_to_users[future]
            meta_stats = future.result()
            if meta_stats is not None:
                batch.append(meta_stats)
    tp2 = time.time()
    print(f'Collection done in: {tp2-tp1}')

    if len(batch) > 0:
        batch = add_utm(batch, boto_session)
        ddb.batch_write_items(app_config.resource_name('analytics'), batch)

    tp3 = time.time()
    print(f'All done in {tp3-tp1}, collection: {tp2-tp1}, utm: {tp3-tp2}')
