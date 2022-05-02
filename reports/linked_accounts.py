import json
import logging

import boto3

from clearvalue import app_config
from clearvalue.lib.search import elastic
from cvcore.model.cv_types import AccountStatus, AccountTypes
from cvcore.store import loaders, DBKeys
from cvutils import cognito_utils, TerminalColors


def print_institutions():
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    active_status = [AccountStatus.ACTIVE.value, AccountStatus.CLOSED.value]

    institutions = {}
    total_linked = 0

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        accounts = loaders.load_user_accounts(uid, load_active_only=False,
                                              ProjectionExpression='HashKey,SortKey,account_status,account_type,closed_at,created_at,institution_id,is_manual,#value',
                                              ExpressionAttributeNames={'#value': 'value'})
        for account in accounts:
            status = account.get('account_status')
            # if status == active_status and account.get('is_manual') is False:
            if status not in active_status:
                continue
            if account.get('is_manual') is True:
                continue

            institution_id = account.get('institution_id')
            if institution_id is None:
                continue

            total_linked += 1
            institution_stat = institutions.get(institution_id)
            if institution_stat is None:
                institution_stat = {'id': institution_id, 'count': 0, 'value': 0}
            institution_stat['count'] += 1
            institution_stat['value'] += float(account['value'])
            institutions[institution_id] = institution_stat

    print(institutions)
    print(f'total accounts {total_linked}, institutions: {len(institutions.keys())}')
    return institutions


def parse_institutions(institutions, boto_session):
    client = elastic.client(boto_session=boto_session)
    keys = [{"_id": k} for k in institutions.keys()]
    results = client.mget({'docs': keys}, index=app_config['elastic']['institution']['index'])
    for doc in results['docs']:
        if doc.get('found') is False:
            print(f'>>>> {doc}')
            continue
        iid = doc['_id']
        source = doc['_source']
        institution = institutions.get(iid)
        print(
            f'{source["name"].replace(",", " ")},{institution["count"]},"${"{:,}".format(int(institution["value"]))}"')


def avg_linked_institutions():
    # active_status = [AccountStatus.ACTIVE.value, AccountStatus.CLOSED.value]
    linked_types = [AccountTypes.SECURITIES_PORTFOLIO.value, AccountTypes.CASH.value,
                    AccountTypes.LOAN.value, AccountTypes.DEPOSIT.value]

    users_linked = []
    users_linked_types = []
    users_linked_inst = []
    total_linked = 0
    total_users = 0

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        accounts = loaders.load_user_accounts(uid,
                                              load_active_only=False,
                                              ProjectionExpression='HashKey,is_manual,account_type,institution_id')
        user_linked_count = 0
        user_linked_types = {}
        user_linked_inst = {}
        total_users += 1

        for account in accounts:
            if account.get('is_manual') is True:
                continue

            account_type = account['account_type']
            if account_type not in linked_types:
                continue

            institution_id = account.get('institution_id')
            if institution_id is None:
                continue

            total_linked += 1
            user_linked_count += 1
            type_data = user_linked_types.get(account_type, 0)
            user_linked_types[account_type] = type_data + 1

            inst_data = user_linked_inst.get(institution_id, 0)
            user_linked_inst[institution_id] = inst_data + 1

        users_linked.append(user_linked_count)
        users_linked_types.append(user_linked_types)
        users_linked_inst.append(user_linked_inst)

    with open('avg_linked_institutions.json', 'w') as fout:
        json.dump({'users_linked': users_linked,
                   'users_linked_types': users_linked_types,
                   'users_linked_inst': users_linked_inst,
                   'total_linked': total_linked,
                   'total_users': total_users}, fout)

    print(f'{TerminalColors.OK_GREEN}All done{TerminalColors.END}')


def parse_avg_linked_institutions():
    with open('avg_linked_institutions.json', 'r') as fin:
        js = json.load(fin)

    total_institutions = sum([len(rec) for rec in js['users_linked_inst']])

    print(f"total users: {js['total_users']}, "
          f"total linked: {js['total_linked']}, "
          f"total institutions: {total_institutions},"
          f"Avg. inst/user: {total_institutions/js['total_users']}, ")



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # institutions = print_institutions()
    # with open('linked_institutions.json', 'w') as f:
    #     f.write(json.dumps(institutions))

    # with open('linked_institutions.json', 'r') as f:
    #     parse_institutions(json.loads(f.read()), profile)

    # avg_linked_institutions()
    parse_avg_linked_institutions()
