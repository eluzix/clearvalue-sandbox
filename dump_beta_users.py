import csv

import boto3

import cvutils
from clearvalue import app_config
from cvanalytics import is_internal_user
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    with open('/Users/uzix/Downloads/beta_users.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['email', 'first name', 'last name', 'Join Date', 'cvuid'])

        for user in loaders.iter_users():
            uid = user[DBKeys.HASH_KEY]
            if is_internal_user(uid):
                continue

            if user.get('beta_user') is not True:
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
            writer.writerow([email, first_name, last_name, join_date, uid])

    print(f'{TerminalColors.OK_GREEN}All done{TerminalColors.END}')
