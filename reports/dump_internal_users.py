import csv

import boto3

from cvanalytics import get_internal_users
from cvutils.config import get_app_config

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    get_app_config().set_stage('prod')

    with open('internal_users.csv', 'w') as fout:
        writer = csv.writer(fout)
        rows = [[uid] for uid in get_internal_users()]
        print(rows)
        writer.writerows(rows)
