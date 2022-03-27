import json

import boto3

import cvutils
from clearvalue import app_config
from cvcore.providers import intercom, sendgridapi
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors, cognito_utils
from cvutils.dynamodb import ddb


def dump_users():
    all_users = []
    for user in loaders.iter_users(active_only=False):
        email = user['email']
        if '@remitly' in email or '@restservicecompany' in email:
            all_users.append(user)

    with open('spam-users.json', 'w') as fout:
        json.dump(all_users, fout)

    print(f'All done, dumped {TerminalColors.OK_CYAN}{len(all_users)}{TerminalColors.END} users')


def _load_users():
    with open('spam-users.json', 'r') as fin:
        return json.load(fin)


def add_to_spam_users(users):
    batch = []
    for user in users:
        uid = user[DBKeys.HASH_KEY]
        batch.append({DBKeys.HASH_KEY: 'SPAM_USERS', DBKeys.SORT_KEY: uid})
        intercom_users = intercom.search_contact(uid, 'external_id')
        if intercom_users['total_count'] > 0:
            intercom_response = intercom.add_tag_to_contact('6559939', intercom_users['data'][0]['id'])
            print(intercom_response)
    print(f'Adding {len(batch)} users to SPAM_USERS')
    ddb.batch_write_items(app_config.resource_name('store'), batch)


def delete_spam_user(user):
    accounts_table_name = app_config.resource_name('accounts')
    store_table_name = app_config.resource_name('store')
    uid = user[DBKeys.HASH_KEY]
    user = ddb.get_item(accounts_table_name, DBKeys.info_key(uid))
    if user.get('status') == 'deleted':
        print(f'User {uid} already deleted skipping')
        return

    email = user['email']
    username = user['cognito_username']
    sendgrid_response = sendgridapi.delete_contact(email=email)
    cognito_response = cognito_utils.disable_user(username)
    intercom_users = intercom.search_contact(uid, 'external_id')
    intercom_response = '-'
    if intercom_users['total_count'] > 0:
        intercom_response = intercom.add_tag_to_contact(app_config['intercom']['deleted.tag'],
                                                        intercom_users['data'][0]['id'])

    ddb.update_with_fields(accounts_table_name, DBKeys.info_key(uid), {'status': 'deleted'}, ['status'])

    delete_request_date = cvutils.today(True)
    delete_request = {
        DBKeys.HASH_KEY: DBKeys.USER_DELETE_REQUEST,
        DBKeys.SORT_KEY: uid,
        'uid': uid,
        'email': email,
        'cognito_username': username,
        'ip': 'missing',
        'status': 'open',
        'spam_user': True,
        'created_at': cvutils.timestamp_from_date(delete_request_date),
    }
    ddb.put_item(store_table_name, delete_request)
    print(f'done deleting {uid}')


if __name__ == '__main__ ':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # add_to_spam_users(_load_users())
    # dump_users()
    all_users = _load_users()
    for user in all_users:
        delete_spam_user(user)

    print(f'All done...')
