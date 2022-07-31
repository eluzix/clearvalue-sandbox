import asyncio
import datetime
import json
from enum import Enum

import boto3

import cvutils
from cvanalytics.daily_report import _should_filter_user
from cvutils import TerminalColors, elastic
from clearvalue import app_config
from cvanalytics import is_user_active, query_cursor
from cvcore.store import DBKeys, loaders
from cvutils.dynamodb import ddb


def users():
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # all_users = [user for user in loaders.iter_users()]
    # stats_users = [user for user in iter_active_users(cvutils.yesterday(), True, active_only=False)]
    #
    # with open('/Users/uzix/Downloads/users.json', 'w') as fout:
    #     json.dump({'stats': stats_users, 'users': all_users}, fout, cls=cvutils.ValueEncoder)

    with open('/Users/uzix/Downloads/users.json', 'r') as fin:
        js = json.load(fin)
        all_users = js['users']
        stats_users = [u for u in js['stats'] if u.get('status') != 'deleted']

    print(f'all size: {len(all_users)}, stats size: {len(stats_users)}')
    actives = 0
    active_ids = set()
    for user in stats_users:
        if is_user_active(user):
            actives += 1
            active_ids.add(user[DBKeys.HASH_KEY])

    client = elastic.client(boto_session=boto_session)
    # dt = cvutils.yesterday()
    # start_date = dt = datetime.timedelta(days=2)
    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                {"match": {"category": "user"}},
                {"match": {"event": "login"}}
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "gte": '2022-05-17',
                        "lt": '2022-05-18',
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }

    logins = 0
    login_ids = set()
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
        source = hit['_source']
        uid = source.get('uid')
        if _should_filter_user(uid):
            continue
        # doc_date = datetime.datetime.strptime(source['ts'], '%Y-%m-%dT%H:%M:%S.%f%z')
        logins += 1
        login_ids.add(uid)

    print(f'total actives {TerminalColors.OK_GREEN}{actives}{TerminalColors.END}')
    print(f'total logins {TerminalColors.WARNING}{logins}{TerminalColors.END}')
    print(login_ids - active_ids)

    # table_name = app_config.resource_name('analytics')
    # count = ddb.get_connection().query(TableName=table_name, Select='COUNT',
    #                                    IndexName='GS1-index',
    #                                    KeyConditionExpression='GS1Hash = :GS1_HASH',
    #                                    ExpressionAttributeValues={
    #                                        ':GS1_HASH': ddb.serialize_value('USER_STATS'),
    #                                    })
    # print(count)
    #
    # kwargs = {
    #     'IndexName': 'GS1-index',
    #     'Select': 'COUNT',
    #     'KeyConditionExpression': f'{DBKeys.GS1_HASH} = :{DBKeys.GS1_HASH}',
    #     'ExpressionAttributeValues': {
    #         f':{DBKeys.GS1_HASH}': ddb.serialize_value('ALL_USERS')
    #     }
    # }
    # table_name = app_config.resource_name('accounts')
    # count = ddb.get_connection().query(TableName=table_name, **kwargs)
    # print(count)

    # with open('/Users/uzix/Downloads/crypto-transactions.csv', 'r') as fin:
    #     reader = csv.reader(fin)
    #     reader.__next__()
    #     uid = 'befba013-635c-4662-a298-ebe163c3f50c'
    #     account_id = '1a0e3e9f-5b74-4387-912b-8fe60b0b83ee'
    #     account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    #     process_crypto_transactions(reader, uid, account)


def _enum():
    class HoldingType(Enum):
        CASH = 'cash'
        DERIVATIVE = 'derivative'
        EQUITY = 'equity'

        @classmethod
        def _missing_(cls, value):
            if value == 'et':
                return cls(HoldingType.EQUITY)

            return super()._missing_(value)

    print(HoldingType('et'))


def migrate_transactions():
    pass
    # queue_url = app_config['sqs']['user.calcs.url']
    # sqs = cvutils.boto3_client('sqs')
    # # .send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))
    #
    # all_users = []
    # for user in loaders.iter_users():
    #     uid = user[DBKeys.HASH_KEY]
    #     all_users.append({
    #         'Id': f'tr-{uid}',
    #         'MessageBody': json.dumps({'uid': uid, 'action': 'migrate-transactions'})
    #     })
    #
    # for chunk in cvutils.grouper(all_users, 10):
    #     chunk = [c for c in chunk if c is not None]
    #     sqs.send_message_batch(QueueUrl=queue_url, Entries=chunk)


def fix_mortgage():
    table_name = app_config.resource_name('accounts')
    account_id = '9624af37-9d9a-4710-9317-0144f484e692'
    all_tps = ddb.query(app_config.resource_name('accounts'),
                        KeyConditionExpression='HashKey = :HashKey AND SortKey >= :SortKey',
                        ExpressionAttributeValues={
                            ':HashKey': ddb.serialize_value(account_id),
                            ':SortKey': ddb.serialize_value('AC:TP:2021-03-03')
                        })
    batch = []
    for tp in all_tps:
        if tp[DBKeys.SORT_KEY].startswith('AC:TP:'):
            print(
                f'{TerminalColors.OK_GREEN}{tp["SortKey"]}{TerminalColors.END} == {TerminalColors.WARNING}{tp["value"]}{TerminalColors.END}')
            tp['old_value'] = tp['value']
            tp['value'] = 0
            batch.append(tp)

    if len(batch) > 0:
        print(f'Updating {len(batch)} items')
        ddb.batch_write_items(table_name, batch)


def amazon():
    data = loaders.load_securities_history(['aapl'], load_since='2014-08-01')
    print(data)


if __name__ == '__main__':
    # print(_load_symbol_news('ASDFASDFASDF'))
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # migrate_transactions()
    # fix_mortgage()
    # loop = asyncio.get_running_loop()
    # loop.run_until_complete(asyncio.create_task(test_async))
    amazon()
