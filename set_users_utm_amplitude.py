import json

import boto3
import requests

import cvutils
from clearvalue import app_config
from cvanalytics import is_internal_user
from cvutils.dynamodb import ddb


def dump_users():
    start_date = int(cvutils.timestamp_from_string('2021-10-15'))
    end_date = int(cvutils.timestamp_from_string('2021-11-12'))
    users_stats = ddb.query(app_config.resource_name('analytics'),
                            IndexName='GS1-index',
                            KeyConditionExpression='GS1Hash = :GS1_HASH',
                            FilterExpression='created_at >= :join_start AND created_at <= :join_end',
                            ExpressionAttributeValues={
                                ':GS1_HASH': ddb.serialize_value('USER_STATS'),
                                ':join_start': ddb.serialize_value(start_date),
                                ':join_end': ddb.serialize_value(end_date),
                            })

    users = []
    for user in users_stats:
        if is_internal_user(user['uid']):
            continue

        user_props = {}
        for k in user:
            if k.startswith('utm_') and k != 'utm_history':
                user_props[k] = user[k]

        if len(user_props) > 0:
            users.append({'user_id': user['uid'], 'user_properties': user_props})
            print(json.dumps({'user_id': user['uid'], 'user_properties': user_props}))

    # response = requests.post('https://api2.amplitude.com/identify', json={
    #     'api_key': app_config['amplitude']['key'],
    #     'identification': users
    # })
    # #
    # if response.status_code != 200:
    #     print(f'Error sending data to amplitude, status_code: {response.status_code}, response: {response.text}')
    # else:
    #     print(f'All done {len(users)} users, response: {response.text}')


if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    dump_users()
