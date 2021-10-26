import boto3

from clearvalue import app_config
from cvutils.store.keys import DBKeys
from cvutils import cognito_utils
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    all_uids = set()
    for user in cognito_utils.iterate_users(primary_identities_only=False):
        all_uids.add(cognito_utils.uid_from_user(user))

    all_keys = [DBKeys.info_key(uid) for uid in all_uids]
    print(f'All keys size {len(all_keys)}')
    users = ddb.batch_get_items(app_config.resource_name('accounts'), all_keys)
    print(f'All users size {len(users)}')
    batch = []
    for user in users:
        if user.get('status') == 'tmp_status_here' or user.get('status') is None:
            user['status'] = 'active'
            
        if user.get('status') != 'active':
            print('>>>>', user.get('status'))
            # continue

        # if user.get(DBKeys.GS2_HASH) == 'ALL_USERS':
        #     continue

        user[DBKeys.GS1_HASH] = 'ALL_USERS'
        user[DBKeys.GS1_SORT] = user[DBKeys.HASH_KEY]
        batch.append(user)

    if len(batch) > 0:
        ddb.batch_write_items(app_config.resource_name('accounts'), batch)

    print(f'All done with {len(batch)} users')
