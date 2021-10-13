import boto3

from clearvalue import app_config
from clearvalue.analytics import is_internal_user
from clearvalue.lib.store import loaders, DBKeys

if __name__ == '__main__':
    # preferred_mfa
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    mfa_data = {}
    for user in loaders.iter_users():
        if is_internal_user(user[DBKeys.HASH_KEY]):
            continue

        mfa_type = user.get('preferred_mfa')
        type_data = mfa_data.get(mfa_type, 0)
        mfa_data[mfa_type] = type_data + 1

    print(mfa_data)
