import boto3

from clearvalue import app_config
from clearvalue.lib import cognito_utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import loaders, DBKeys

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)

        accounts = loaders.load_user_accounts(uid)
        user_count = 0
        for ac in accounts:
            if ac.get('is_manual') == False:
                user_count += 1

        if user_count > 0:
            u = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key(uid))

            print(f'{uid}, {u["email"]}, {user_count}')

    print('All done')