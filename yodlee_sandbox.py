import json

import boto3

from clearvalue import app_config
from clearvalue.lib.store import loaders, DBKeys

# def generate_token(uid):
#     yodlee_id = loaders.load_yodlee_id(uid)
#     if yodlee_id is None:
#         table_name = app_config.resource_name('accounts')
#         key = DBKeys.info_key(uid)
#         user = ddb.get_item(table_name, key, ProjectionExpression='email')
#
#         # if yodlee_id is None than we need to
#         yodlee_id = utils.generate_id()
#         payload = lambda_utils.invoke({'yodlee_id': yodlee_id, 'email': user['email']}, 'yodlee.register')
#         print(payload)
#         user['yodlee_internal_id'] = payload['yodlee_internal_id']
#
#         user['yodlee_id'] = yodlee_id
#         user[DBKeys.GS2_HASH] = 'PRVD:LOGIN:{}'.format(user['yodlee_id'])
#         user[DBKeys.GS2_SORT] = DBKeys.INFO
#
#         ddb.update_with_fields(table_name, key, user, ['yodlee_id', 'yodlee_internal_id', DBKeys.GS2_HASH, DBKeys.GS2_SORT])
#
#     return yodlee.jwt_token(yodlee_id)


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    # with open('sandbox.json', 'r') as f:
    #     holdings = json.load(f)

    # for h in holdings:
    #     print(yodlee._normalize_holding(h))

    uid = 'c18cb535-403a-478f-ba24-805c403ed1fb'
    print(loaders.load_yodlee_id(uid))
    # uid = 'befba013-635c-4662-a298-ebe163c3f50c'
    # uid = '2bb40134-1a88-4491-bedf-496401a429f0'
    # payload = lambda_utils.invoke({'uid': uid, 'full_fetch': True}, 'yodlee.process', invocation_type='Event')
    # print(payload)
