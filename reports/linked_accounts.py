import json
import logging

import boto3

from clearvalue import app_config
from clearvalue.graphql.data_loaders import InstitutionLoader
from clearvalue.lib import cognito_utils
from clearvalue.lib.search import elastic
from clearvalue.lib.store import loaders
from clearvalue.model.cv_types import AccountStatus


def print_institutions():
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

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
        print(f'{source["name"].replace(",", " ")},{institution["count"]},"${"{:,}".format(int(institution["value"]))}"')



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # institutions = print_institutions()
    # with open('linked_institutions.json', 'w') as f:
    #     f.write(json.dumps(institutions))

    with open('linked_institutions.json', 'r') as f:
        parse_institutions(json.loads(f.read()), profile)
