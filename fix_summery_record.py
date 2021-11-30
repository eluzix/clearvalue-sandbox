import boto3

from clearvalue import app_config
from cvcore.store.keys import DBKeys
from cvutils import boto3_client, cognito_utils
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    dev_profile = boto3.session.Session(profile_name='clearvalue-stage-sls')
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')

    queue_url = app_config['sqs']['user.calcs.url']

    sqs = boto3_client('sqs')

    batch = []
    accounts_table = app_config.resource_name('accounts')
    for user in cognito_utils.iterate_users():
        uid = user['Username']
        hash_key = DBKeys.summary_key(uid)
        summery = ddb.get_item(accounts_table, {DBKeys.HASH_KEY: uid,
                                                DBKeys.SORT_KEY: 'SUMMERY'})
        summery[DBKeys.SORT_KEY] = DBKeys.SUMMARY
        ddb.put_item(accounts_table, summery)
