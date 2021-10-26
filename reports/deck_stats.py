import datetime
import decimal
import enum
import json
import logging
import statistics

import boto3

import cvutils as utils
from clearvalue import app_config
from clearvalue.lib.search import elastic
from clearvalue.lib.store import loaders
from clearvalue.analytics import query_cursor
from cvutils import cognito_utils

_IGNORED_USERS = {'0abbd698-5258-4499-882a-4980dccf11c7', '0cc776bd-3d82-4b21-8c7a-7f278be28206', '0d598d67-dd2f-46f6-8b5b-06e725f00199', '0f02cee3-7ba9-4af1-a127-d82d43d0a4bd',
                  '2c735335-d41e-4816-8cab-354266f917dc', '2fdd3491-524e-4f56-9eeb-e41fa2e802d4', '35cc142c-3656-4329-88f2-1a66f944231b', '379b90ee-db97-4462-9285-063abc0f33ce',
                  '3dd00b8d-4496-48e6-9f96-9d14b0f91993', '40b03d9c-39a6-4ff7-ac59-2ae0217b06de', '49040d17-fc6e-4c76-ad9e-85c51142f64a', '4aaa981b-004b-4c39-a743-979ee062ddee',
                  '4c4893e0-87fc-4a6b-b1a7-e5abe9bd410a', '53d7815e-aa95-4692-a9b5-5df8878e96a7', '59447c30-318b-4970-aa70-ba19ca968322', '5e5f0619-7306-46e7-85be-b521ee79f5ad',
                  '665574ea-cce9-4708-8a08-579033f6e689', '673e7d2c-4508-4658-9303-0646669d65f6', '68fcdcf0-7a22-43fe-a975-e111a751c380', '74f61647-7eff-4522-ad8b-a8349ee4e67c',
                  '7f58b698-59d7-42de-9c7d-a9e4e8d88573', '81131bbe-24c2-4179-930e-53ea7044a336', '831721e6-e376-4dd1-9b78-6531ac359f7e', '8e69e680-7f86-43b8-bf96-35e6f31a8daa',
                  '8f384a80-6de1-4e5e-8a8d-ce057fab91b9', '917d5900-ef7a-4655-af7d-d85b85acd3a8', 'a718845d-f760-48e6-ab93-d9464d1cbc27', 'b50aed82-99e9-4977-84bf-7e795245ad8c',
                  'b9cd6c4a-b735-4b28-97a9-fae6c009be79', 'bdb47534-7ee2-42fc-8125-1890bed0cc7e', 'c2a2d4a8-a21a-4404-a781-4a9a58d48c3f', 'd47699f2-82a0-4996-87e0-2aea300bbef9',
                  'd808a785-6eaf-40a1-a414-18be665948da', 'd8ff97b1-58a5-431c-afe6-434752e37c09', 'dd9883c5-e309-4099-9714-b80f7d1d5a33', 'f0bf233b-8155-451f-a1a0-bb466351709b',
                  'ffec818d-8a3d-4ec2-a828-74cd390ed79c', '8afbdbce-972e-45a7-bc89-72c8e1a68771', 'fba227c5-f6e9-4bd6-8140-eeeadd093f56', '2bb40134-1a88-4491-bedf-496401a429f0',
                  'd05456d6-2fa3-418f-b555-b3b82e24a936', '7b34bcda-26a5-4a3f-afbe-9153838110f0', 'b94a206e-819b-41d5-9370-86948f21db7b', '8e25f53d-3c31-4989-b395-844331ca3760',
                  '0089a759-e4cd-4e3a-89a9-4fd0d1099437', '618e846c-8418-4c18-814f-7aa1cce8b0f1', '32b60a64-0773-43a8-a262-29aaff223f5d', '4004bddf-4946-483d-8c95-3b9082380d81',
                  'd792a1c6-1da0-403b-ae29-37feb52630c0', '32b60a64-0773-43a8-a262-29aaff223f5d', 'd792a1c6-1da0-403b-ae29-37feb52630c0', '59121dad-e188-4e37-8ddc-e56d97a2fcfd',
                  'dc14c385-5c75-4a41-b0fb-fc527d37a6d2', 'ce1576ce-2343-40e5-9289-d7ac58b29472'}

_30_DAYS_START = (utils.today() - datetime.timedelta(days=30)).replace(tzinfo=datetime.timezone.utc).timestamp()


class ValueEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        if isinstance(o, enum.Enum):
            return o.value

        if isinstance(o, datetime.datetime):
            # return o.strftime('%Y-%m-%dT%H:%M:%S.%f')
            return o.replace(tzinfo=datetime.timezone.utc).timestamp()

        return super(ValueEncoder, self).default(o)


def __user(users, hit, session_window):
    source = hit['_source']
    uid = source.get('uid')
    if uid is None:
        return users

    doc_date = datetime.datetime.strptime(source['ts'], '%Y-%m-%dT%H:%M:%S.%f%z')

    user = users.get(uid)
    if user is None:
        user = {'sessions': [], 'session_start': doc_date, 'session_end': doc_date}

    session_end = user['session_end']
    diff = doc_date - session_end
    if diff.total_seconds() > session_window:
        user['sessions'].append({'session_start': user['session_start'], 'session_end': user['session_end']})
        user['session_start'] = doc_date

    user['session_end'] = doc_date
    users[uid] = user

    return users

def _active_users(boto_session=None):
    client = elastic.client(boto_session=boto_session)
    users = {}

    # buckets = {
    #     '30days': set(),
    #     'avg_age_30days': [],
    #
    #     '7days': set(),
    #     'avg_age_7days': [],
    #
    #     '1day': set(),
    #     'avg_age': [],
    # }

    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                # {"match": {"event": "view"}}
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "format": "strict_date_optional_time",
                        "gte": "2020-10-01T00:00:00.000Z",
                        "lt": "2020-11-01T00:00:00.000Z"
                    }
                }
            }],
        },
    }

    session_window = 60 * 30

    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts'], 'sort': [{'ts': 'asc'}]}, index='app-logs*'):
        users = __user(users, hit, session_window)

    query['bool']['filter'][0]['range']['ts']['gte'] = '2020-11-01T00:00:00.000Z'
    query['bool']['filter'][0]['range']['ts']['lt'] = '2020-12-01T00:00:00.000Z'
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts'], 'sort': [{'ts': 'asc'}]}, index='app-logs*'):
        users = __user(users, hit, session_window)

    query['bool']['filter'][0]['range']['ts']['gte'] = '2020-12-01T00:00:00.000Z'
    query['bool']['filter'][0]['range']['ts']['lt'] = '2021-01-01T00:00:00.000Z'
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts'], 'sort': [{'ts': 'asc'}]}, index='app-logs*'):
        users = __user(users, hit, session_window)

    query['bool']['filter'][0]['range']['ts']['gte'] = '2021-01-01T00:00:00.000Z'
    query['bool']['filter'][0]['range']['ts']['lt'] = '2021-02-01T00:00:00.000Z'
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts'], 'sort': [{'ts': 'asc'}]}, index='app-logs*'):
        users = __user(users, hit, session_window)

    for uid in users:
        if len(users[uid]['sessions']) == 0:
            start = users[uid]['session_start']
            end = users[uid]['session_end']
            if start != end:
                users[uid]['sessions'].append({'session_start': start, 'session_end': end})

    with open('users_sessions.json', 'w') as f:
        f.write(json.dumps(users, cls=ValueEncoder))

    return users


def filter_users(users, min_sessions, exclude_us=False, last_login_30_days=False):
    ret = []
    for uid in users:
        # if exclude_us and uid in _IGNORED_USERS:
        #     continue

        user = users[uid]

        if len(user['sessions']) >= min_sessions:
            if last_login_30_days:
                for s in user['sessions']:
                    if s['session_start'] >= _30_DAYS_START:
                        ret.append({'uid': uid, 'sessions': user['sessions']})
                        break
            else:
                ret.append({'uid': uid, 'sessions': user['sessions']})

    return ret


def session_stats(users):
    stats = {'total_sessions': 0}
    sessions_avg = []
    sessions_num_avg = []

    for user in users:
        sessions = [s['session_end'] - s['session_start'] for s in user['sessions']]
        stats['total_sessions'] += len(sessions)
        if len(sessions) > 0:
            avg_session = statistics.mean(sessions)
        if avg_session < 2000:
            sessions_avg.append(avg_session)
            sessions_num_avg.append(len(sessions))

    stats['avg_session_time'] = statistics.mean(sessions_avg)
    stats['min_session_time'] = min(sessions_avg)
    stats['max_session_time'] = max(sessions_avg)
    stats['avg_num_of_sessions'] = statistics.mean(sessions_num_avg)

    return stats


def users_stats(users):
    total_users = len(users)
    stats = {'total_accounts': 0, 'total_linked': 0, 'total_aum': 0, 'linked_aum': 0}
    # active_status = AccountStatus.ACTIVE.value

    users_accounts = []
    users_linked_accounts = []
    users_aum = []
    institution_map = {}

    for user in users:
        uid = user['uid']
        accounts = loaders.load_user_accounts(uid,
                                              ProjectionExpression='HashKey,SortKey,account_status,account_type,closed_at,created_at,is_manual,institution_id,#value',
                                              ExpressionAttributeNames={'#value': 'value'})

        total_user_aum = 0
        total_user_linked_value = 0
        total_user_linked_count = 0
        total_user_account_count = 0
        user_institution_map = {}

        for account in accounts:
            # status = account.get('account_status', active_status)
            total_user_account_count += 1

            if account.get('is_manual') is False:
                total_user_linked_value += float(account['value'])
                total_user_linked_count += 1

                inst_data = user_institution_map.get(account['institution_id'], 0)
                inst_data += 1
                user_institution_map[account['institution_id']] = inst_data

            total_user_aum += float(account['value'])

        if 100000000 > total_user_aum > 0:
            stats['total_accounts'] += total_user_account_count
            stats['total_aum'] += total_user_aum
            stats['linked_aum'] += total_user_linked_value

            users_accounts.append(total_user_account_count)
            if total_user_linked_count > 0:
                stats['total_linked'] += total_user_linked_count
                users_linked_accounts.append(total_user_linked_count)
            users_aum.append(total_user_aum)

            for inid in user_institution_map:
                inst_data = institution_map.get(inid, 0)
                inst_data += user_institution_map[inid]
                institution_map[inid] = inst_data

    # stats['avg_num_of_accounts'] = statistics.mean(users_accounts)
    stats['avg_num_of_accounts'] = sum(users_accounts)/total_users
    # stats['avg_num_of_linked_accounts'] = statistics.mean(users_linked_accounts)
    stats['avg_num_of_linked_accounts'] = sum(users_linked_accounts)/total_users
    stats['avg_aum'] = sum(users_aum)/total_users
    stats['min_aum'] = min(users_aum)
    stats['max_aum'] = max(users_aum)
    # stats['avg_aum'] = stats['total_aum']/stats['total_linked']
    stats['linked_aum'] = stats['linked_aum']
    stats['avg_linked_aum'] = stats['linked_aum'] / total_users

    print(institution_map)
    stats['total_institutions'] = len(institution_map.keys())

    return stats


def all_stats(users):
    stats = {'total_users': len(users)}
    stats.update(session_stats(users))
    stats.update(users_stats(users))
    # stats['total_users'] = len(users)
    print(f"""
Total users: {stats['total_users']}
Total sessions: {stats['total_sessions']}
Avg. session time: {round(stats['avg_session_time'] / 60, 2)}m
Min. session time: {round(stats['min_session_time'] / 60, 2)}m
Max. session time: {round(stats['max_session_time'] / 60, 2)}m
Avg. number of sessions: {round(stats['avg_num_of_sessions'], 2)}

Total assets: {stats['total_accounts']}
Total linked assets: {stats['total_linked']}
Avg. number of assets: {round(stats['avg_num_of_accounts'], 2)}
Avg. number of linked assets: {round(stats['avg_num_of_linked_accounts'], 2)}
Total AUM: ${"{:,}".format(int(stats['total_aum']))}
Min AUM: ${"{:,}".format(int(stats['min_aum']))}
Max AUM: ${"{:,}".format(int(stats['max_aum']))}
Avg. AUM: ${"{:,}".format(int(stats['avg_aum']))}

Linked AUM: ${"{:,}".format(int(stats['linked_aum']))}
Avg. Linked AUM: ${"{:,}".format(int(stats['avg_linked_aum']))}

Total institutions: {stats["total_institutions"]}
""")


def find_active(users, boto_session=None):
    client = elastic.client(boto_session=boto_session)

    uids = set([u['uid'] for u in users])

    buckets = {
        '30days': set(),
        'avg_age_30days': [],

        '7days': set(),
        'avg_age_7days': [],

        '1day': set(),
        'avg_age': [],
    }

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
                        "gte": '2020-12-08',
                        # "gte": '2021-01-01',
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        },
    }

    signed_in = set()
    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
        source = hit['_source']
        uid = source.get('uid')
        if uid is None:
            continue

        if uid in uids:
            signed_in.add(uid)

    print(f'total {len(uids)} users and {len(signed_in)} logged-in')


def our_users():
    ret = []
    for user in cognito_utils.iterate_users():
        email = cognito_utils.user_attribute(user, 'email')
        skip_user = True
        for p in ['claritus', 'clearvalue', 'groupbwt', 'shai+', 'gabriel', 'uzi+', 'eluzix', 'refaeli']:
            if p in email:
                skip_user = False
                break
        if skip_user:
            continue

        ret.append(cognito_utils.uid_from_user(user))

    return ret


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    users = _active_users(boto_session=profile)

    # print(our_users())

    # with open('users_sessions.json', 'r') as f:
    #     users = json.loads(f.read())

    # all_stats(filter_users(users, 0, exclude_us=True))

    # print([u['uid'] for u in filter_users(users, 3)])
    # print(len(filter_users(users, 0)))
    # print(len(filter_users(users, 1)))

    # all_stats(users)

    # find_active(filter_users(users, 3), profile)
    # find_active(filter_users(users, 5), profile)
