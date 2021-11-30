import csv
import datetime

import boto3
import dateutil.parser

from clearvalue import app_config
from cvutils import cognito_utils

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    file_name = 'recurring users.csv'

    with open(file_name, 'r') as f:
        reader = csv.reader(f)
        users = {r[0]: r for r in reader}

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        user_row = users.get(uid)
        if user_row is not None:
            creation_date = user['UserCreateDate']
            update_date = user['UserLastModifiedDate']
            name = ''
            for att in user['Attributes']:
                if att['Name'] == 'name':
                    name = att['Value']
                    break
            user_row.insert(1, name)
            user_row.insert(3, creation_date.strftime('%m/%d/%Y'))
            users[uid] = user_row
            # user_row.append(utils.date_to_str(update_date))

    final_users = [users[k] for k in users]
    for u in final_users:
        if len(u) == 3:
            u.insert(1, '')
            u.insert(3, '')
        else:
            d = dateutil.parser.parse(u[4])
            u[4] = d.strftime('%m/%d/%Y %H:%M:%S')
    final_users.sort(key=lambda u: int(u[2]), reverse=True)

    with open('{}_enriched.csv'.format(file_name), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(final_users)

    print('*** All Done ***')
