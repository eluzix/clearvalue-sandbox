import datetime
import json
from enum import Enum

import boto3

import cvutils
from clearvalue.gql.schema.providers import _load_symbol_news
from cvanalytics.daily_report import _should_filter_user
from cvutils import TerminalColors, elastic
from clearvalue import app_config
from cvanalytics import iter_active_users, is_user_active, query_cursor
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


if __name__ == '__main__':
    print(_load_symbol_news('ASDFASDFASDF'))
