import boto3

from clearvalue import app_config
from clearvalue.lib import cognito_utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys


def accounts():
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    total_value = 0
    user_count = 0
    users = set()
    providers = set()
    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)

        accounts = loaders.load_user_accounts(uid, load_active_only=False)
        for ac in accounts:
            if ac.get('is_manual') is False:
                total_value += float(ac.get('value', 0))
                user_count += 1
                users.add(ac.get('uid'))
                providers.add(ac.get('institution_id'))

    print(f'total_value: {total_value}')
    print(f'total accounts: {user_count}')
    print(f'total users: {len(users)}')
    print(f'total providers: {len(providers)}')
    print(f'All done')


def pains():
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    pains = {}
    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)

        user = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key(uid))
        if user is not None:
            join_pains = user.get('join_pains')
            if join_pains is not None:
                for pain in join_pains:
                    count = pains.get(pain, 0)
                    count += 1
                    pains[pain] = count

    print(f'pains: {pains}')
    print(f'All done')

    # {
    #     'in-control': 82,
    #     'inform-family': 23,
    #     'learn-from-peers': 20,
    #     'future-wealth': 43,
    #     'managing-wealth': 15,
    #     'maximize-investments': 45,
    #     'spreadsheet-pain': 13
    # }

    
