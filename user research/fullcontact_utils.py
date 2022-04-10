import csv
import json

import boto3
from fullcontact import FullContactClient

import cvutils
from clearvalue import app_config
from cvanalytics import iter_active_users, is_user_active, ACTIVE_GROUPS, query_cursor
from cvcore.store import DBKeys
from cvutils import TerminalColors, elastic, cognito_utils
from cvutils.dynamodb import ddb


# def dump_active_users():
#     active_group = ACTIVE_GROUPS[3]
#     headers = 'Record ID,Email 1,Email 2,Email 3,First Name,Last Name,Address Line 1,Address Line 2,City,State,Zip Code,Primary Phone Number,Secondary Phone Number,LinkedIn Profile URL,Twitter Profile URL'
#     active_keys = []
#     for user in iter_active_users(load_latest=True, active_only=False):
#         if is_user_active(user, active_group):
#             active_keys.append(DBKeys.info_key(user[DBKeys.HASH_KEY]))
#
#     all_users = ddb.batch_get_items(app_config.resource_name('accounts'), active_keys)
#     with open('full_contact.json', 'w') as fout:
#         json.dump(all_users, fout)
#
#     with open('full_contact.csv', 'w') as fout:
#         writer = csv.writer(fout)
#         headers = headers.split(',')
#         writer.writerow(headers)
#         for i, user in enumerate(all_users):
#             writer.writerow([str(id), user['email']])
#
#     print(f'{TerminalColors.OK_GREEN}All Done{TerminalColors.END}')


# def fix_active(boto_session):
#     client = elastic.client(boto_session=boto_session)
#     query = {
#         'bool': {
#             "must": [
#                 {"match": {"log_name": "yodlee"}},
#                 {"match": {"event": "register_user"}},
#             ],
#             'filter': [{
#                 "range": {
#                     "ts": {
#                         "gte": '2020-01-01',
#                         'format': 'yyyy-MM-dd'
#                     }
#                 }
#             }],
#         }
#     }
#
#     yodlee_data = {}
#     for hit in query_cursor(client, query):
#         source = hit['_source']
#         yodlee_data[source['email']] = source
#
#     with open('all_actives_full_email.json', 'r') as fin:
#         all_users = json.load(fin)
#
#     for user in all_users:
#         email = user.get('email')
#         if email is None:
#             print(f'Missing email for {TerminalColors.FAIL}{user[DBKeys.HASH_KEY]}{TerminalColors.END}')
#             continue
#
#         user_info = yodlee_data.get(email)
#         if user_info is None:
#             print(f'Missing yodlee data for {TerminalColors.WARNING}{user[DBKeys.HASH_KEY]}, {email}{TerminalColors.END}')
#             continue
#
#         user['yodlee_internal_id'] = user_info['yodlee_internal_id']
#         user['yodlee_id'] = user_info['yodlee_id']
#         user[DBKeys.GS2_HASH] = 'PRVD:LOGIN:{}'.format(user['yodlee_id'])
#         user[DBKeys.GS2_SORT] = DBKeys.INFO
#
#     with open('all_actives_fixed.json', 'w') as fout:
#         json.dump(all_users, fout)
#
#     #     all_uids = [DBKeys.info_key(u['HashKey']) for u in all_users]
#
#     # print(f'all active users: {len(all_users)}')
#     # for user in all_users:
#     #     uid = user[DBKeys.HASH_KEY]
#     #     ddb.update_with_fields(app_config.resource_name('accounts'), DBKeys.info_key(uid),
#     #                            {'yodlee_id': 'NOT_SET'}, ['yodlee_id'])
#
#     # all_users = ddb.batch_get_items(app_config.resource_name('accounts'), all_uids)
#     # for
#
#     # with open('all_actives.json', 'w') as fout:
#     #     json.dump(all_users, fout)
#
#     # all_users = {}
#     # for user in cognito_utils.iterate_users():
#     #     cuid = cognito_utils.uid_from_user(user)
#     #     if cuid in all_uids:
#     #         new_user = {
#     #             DBKeys.HASH_KEY: cuid,
#     #             DBKeys.SORT_KEY: DBKeys.INFO,
#     #             'uid': cuid,
#     #             'email': cognito_utils.user_attribute(user, 'email'),
#     #             'email_verified': True,
#     #             'created_at': cvutils.timestamp_from_date(user['UserCreateDate']),
#     #             'cognito_username': user['Username'],
#     #             DBKeys.GS1_HASH: 'ALL_USERS',
#     #             DBKeys.GS1_SORT: cuid
#     #         }
#     #         name = cognito_utils.user_attribute(user, 'name')
#     #         if name is not None:
#     #             new_user['name'] = name
#     #
#     #         given_name = cognito_utils.user_attribute(user, 'given_name')
#     #         if given_name is not None:
#     #             new_user['given_name'] = given_name
#     #
#     #         family_name = cognito_utils.user_attribute(user, 'family_name')
#     #         if family_name is not None:
#     #             new_user['family_name'] = family_name
#     #             # email_list_contact['last_name'] = family_name
#     #
#     #         phone_number = cognito_utils.user_attribute(user, 'phone_number')
#     #         if phone_number is not None:
#     #             user['phone_number'] = phone_number
#     #
#     #         all_users[cuid] = new_user
#     # batch = [all_users[u] for u in all_users]
#     # ddb.batch_write_items(app_config.resource_name('accounts'), batch)

# def write_fixed():
#     with open('all_actives_fixed.json', 'r') as fin:
#         all_users = json.load(fin)
#
#     batch = [user for user in all_users]
#     # for user in all_users:
#     #     if DBKeys.GS2_HASH in user:
#     #         batch.append(user)
#
#     print(batch)
#     # ddb.batch_write_items(app_config.resource_name('accounts'), batch)
#     print(f'>>>>>{len(batch)}')


# def fix_empty_emails():
#     with open('all_actives_full_email.json', 'r') as fin:
#         all_users = json.load(fin)
#
#     for user in all_users:
#         email = user.get('email')
#         if email is None:
#             uid = user[DBKeys.HASH_KEY]
#             print(f'Missing email for {TerminalColors.FAIL}{uid}{TerminalColors.END}')
#             cu = cognito_utils.get_user_by_uid(uid)
#             new_user = {
#                 DBKeys.HASH_KEY: uid,
#                 DBKeys.SORT_KEY: DBKeys.INFO,
#                 'uid': uid,
#                 'email': cognito_utils.user_attribute(cu, 'email', attr_field='UserAttributes'),
#                 'email_verified': True,
#                 'created_at': cvutils.timestamp_from_date(cu['UserCreateDate']),
#                 'cognito_username': cu['Username'],
#                 DBKeys.GS1_HASH: 'ALL_USERS',
#                 DBKeys.GS1_SORT: uid
#             }
#             name = cognito_utils.user_attribute(cu, 'name', attr_field='UserAttributes')
#             if name is not None:
#                 new_user['name'] = name
#
#             given_name = cognito_utils.user_attribute(cu, 'given_name', attr_field='UserAttributes')
#             if given_name is not None:
#                 new_user['given_name'] = given_name
#
#             family_name = cognito_utils.user_attribute(cu, 'family_name', attr_field='UserAttributes')
#             if family_name is not None:
#                 new_user['family_name'] = family_name
#
#             phone_number = cognito_utils.user_attribute(cu, 'phone_number', attr_field='UserAttributes')
#             if phone_number is not None:
#                 new_user['phone_number'] = phone_number
#
#             user.update(new_user)
#
#     with open('all_actives_full_email.json', 'w') as fout:
#         json.dump(all_users, fout)

def fix_csv():
    rows = []
    with open('full_contact.csv', 'r') as fin:
        reader = csv.reader(fin)
        rows.append(reader.__next__())
        counter = 1
        for row in reader:
            row[0] = str(counter)
            rows.append(row)
            counter += 1

    with open('full_contact.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerows(rows)


def process_full_contact_response():
    new_data = []
    with open('full_contact_response.json', 'r') as fin:
        js = json.load(fin)
        for email in js:
            if js[email].get('status') == 404:
                continue
            new_data.append(js[email])

    with open('full_contact_response_only.json', 'w') as fout:
        json.dump(new_data, fout)

    print(f'{TerminalColors.OK_GREEN}All Done{TerminalColors.END}')



if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # dump_active_users()
    # fix_active(boto_session)
    # write_fixed()
    # fix_empty_emails()

    # fix_csv()

    # all_success_data = {}
    # api_key = 'mRuFj8wJBXK7g4uGgARkNA99dqd1BLi8'
    # client = FullContactClient(api_key)
    # with open('full_contact.csv', 'r') as fin:
    #     reader = csv.reader(fin)
    #     reader.__next__()
    #     for row in reader:
    #         try:
    #             email = row[1]
    #             ret = client.person.enrich(email=email)
    #             if ret.is_successful:
    #                 all_success_data[email] = ret.response.json()
    #         except Exception as e:
    #             print(e)
    #
    # with open('full_contact_response.json', 'w') as fout:
    #     json.dump(all_success_data, fout)

    # ret = client.person.enrich(email='jgeskey@verizon.net')
    # print(ret)
    # if ret.is_successful:
    #     print(ret.response.json())

    process_full_contact_response()
