import boto3

from clearvalue import app_config
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    uid = 'cc707940-7853-4e9d-a8b6-56fa839e8c47'
    account_id = '213ad1a5-8adb-4cad-bf28-f5691316444a'
    table_name = app_config.resource_name('accounts')

    all_tps = ddb.query(table_name,
                        KeyConditionExpression='HashKey = :HashKey AND begins_with(SortKey, :SortKey)',
                        ExpressionAttributeValues={
                            ':HashKey': ddb.serialize_value(account_id),
                            ':SortKey': ddb.serialize_value('AC:TP:')
                        })
    for tp in all_tps:
        tp['value'] = 757737
        if 'loan_id' in tp:
            del tp['loan_id']
        if 'loan_value' in tp:
            del tp['loan_value']
        # print(f'{tp[DBKeys.SORT_KEY]} value {tp["value"]}')

    # ddb.batch_write_items(table_name, all_tps)
