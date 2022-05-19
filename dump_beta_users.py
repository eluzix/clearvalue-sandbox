import csv

import boto3

import cvutils
from clearvalue import app_config
from cvanalytics import is_internal_user
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors


def _load_file(filename):
    users = {}
    with open(filename, 'r') as fin:
        # 6 = email, 8 = uid
        reader = csv.reader(fin)
        reader.__next__()
        for row in reader:
            users[row[8]] = row[6]

    return users


def beta_dump():
    unsubscribed_users = _load_file('/Users/uzix/Downloads/Claritus_Users_456484_export_2022-05-11_08_20.csv')
    spam_users = _load_file('/Users/uzix/Downloads/Claritus_Users_456493_export_2022-05-11_08_36.csv')
    enemies = {'4616564c-f30e-4e70-bbfe-3a011b7e3e8e', '661de7d0-a725-4c63-82b6-6e12fbcb09d0',
               '13f4c524-a7c9-4333-8266-48478affc8a4'}

    with open('/Users/uzix/Downloads/beta_users.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['email', 'first name', 'last name', 'Join Date', 'cvuid', 'Intercom unsubscribed'])

        for user in loaders.iter_users():
            uid = user[DBKeys.HASH_KEY]
            if is_internal_user(uid):
                continue

            if user.get('beta_user') is not True:
                continue

            if spam_users.get(uid) is not None:
                continue

            if uid in enemies:
                continue

            email = user['email']
            first_name = user.get('given_name')
            last_name = user.get('family_name')
            if first_name is None:
                name = user.get('name')
                if name is not None:
                    name = name.split()
                    first_name = name[0]
                    if first_name != name[-1]:
                        last_name = name[-1]
            if first_name is None:
                first_name = email

            join_date = cvutils.date_from_timestamp(user['created_at']).strftime('%Y-%m-%d %H:%M:%S')
            unsubscribed = 'true' if unsubscribed_users.get(uid) is not None else 'false'
            writer.writerow([email, first_name, last_name, join_date, uid, unsubscribed])

    print(f'{TerminalColors.OK_GREEN}All done{TerminalColors.END}')


def generate_email_only_list():
    unsubscribed_users = _load_file('/Users/uzix/Downloads/Claritus_Users_461065_export_2022-05-18_17_52.csv')
    print(unsubscribed_users)
    with open('/Users/uzix/Downloads/unsubscribed_users.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['email'])
        for uid in unsubscribed_users:
            writer.writerow([unsubscribed_users[uid]])
    print(f'{TerminalColors.OK_GREEN}All done{TerminalColors.END}')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    generate_email_only_list()
