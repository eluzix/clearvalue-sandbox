import csv

import boto3

from clearvalue import app_config
from cvcore.store import DBKeys
from cvutils.dynamodb import ddb


def dump_all_tags():
    kwargs = {
        'IndexName': 'GS1-index',
        'KeyConditionExpression': f'{DBKeys.GS1_HASH} = :{DBKeys.GS1_HASH}',
        'ExpressionAttributeValues': {
            f':{DBKeys.GS1_HASH}': ddb.serialize_value(DBKeys.ALL_TAGS)
        }
    }
    table_name = app_config.resource_name('accounts')
    with open('all_tags.csv', 'w') as fout:
        writer = csv.writer(fout)
        for item in ddb.query(table_name, **kwargs):
            writer.writerow([item['name']])

    print('All Done')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    dump_all_tags()
