import csv
import json
import os

import boto3

import cvutils
from clearvalue import app_config
from cvcore.providers import intercom, sendgridapi
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors, cognito_utils
from cvutils.dynamodb import ddb


def dump_users():
    all_users = []
    since_date = cvutils.date_from_str('2022-03-28')

    with open(os.path.join(os.path.dirname(__file__), '..', '..', 'clearvalue-api', 'resources', 'junk-mail.json'),
              'r') as fin:
        exclude_domains = json.load(fin)

    for user in loaders.iter_users():
        created_at = cvutils.date_from_timestamp(user['created_at'])
        if created_at < since_date:
            continue

        # exclude_domains.insert(0, 'yahoo.com')
        email = user['email']
        for domain in exclude_domains:
            if domain in email:
                all_users.append(user)
                break

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
        # intercom_users = intercom.search_contact(uid, 'external_id')
        # if intercom_users['total_count'] > 0:
        #     intercom_response = intercom.add_tag_to_contact('6559939', intercom_users['data'][0]['id'])
        #     print(intercom_response)
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
    sendgridapi.delete_contact(email=email)
    cognito_utils.disable_user(username)
    intercom_users = intercom.search_contact(uid, 'external_id')
    if intercom_users['total_count'] > 0:
        intercom.add_tag_to_contact(app_config['intercom']['deleted.tag'],
                                    intercom_users['data'][0]['id'])
        intercom.add_tag_to_contact('6559939', intercom_users['data'][0]['id'])

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
    print(f'done deleting {uid} ({email})')


def print_emails():
    for user in _load_users():
        print(user['email'], user[DBKeys.HASH_KEY])


def dump_all_deleted_to_spam():
    batch = []
    delete_requests = loaders.load_user_delete_requests(only_open=False)
    for request in delete_requests:
        if request.get('spam_user') is True:
            batch.append({DBKeys.HASH_KEY: 'SPAM_USERS', DBKeys.SORT_KEY: request['uid']})

    print(f'Dumping {len(batch)} users to SPAM_USERS')
    ddb.batch_write_items(app_config.resource_name('store'), batch)


def delete_from_klaviyo():
    users = {u['email']: u for u in loaders.iter_users()}
    with open('/Users/uzix/Downloads/spam_users_from_klaviyo.csv', 'r') as fin:
        reader = csv.reader(fin)
        reader.__next__()
        count = 0
        for row in reader:
            spam_user = users.get(row[0])
            if spam_user is None:
                print(f'Skipping {TerminalColors.WARNING}{row[0]}{TerminalColors.END}')
                continue

            delete_spam_user(spam_user)
            count += 1

        print(f'Deleted {TerminalColors.OK_GREEN}{count}{TerminalColors.END} users')

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # print_emails()
    # add_to_spam_users(_load_users())
    # dump_users()
    dump_all_deleted_to_spam()
    # all_users = _load_users()
    # for user in _load_users():
    #     delete_spam_user(user)

    # delete_from_klaviyo()


    print(f'All done...')
