import csv
import time

import boto3

from clearvalue import app_config
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors
from cvutils.dynamodb import ddb


def dump_users_for_lookalike():
    tp1 = time.time()
    users = []
    meta_keys = []
    for user in loaders.iter_users():
        users.append(user)
        meta_keys.append(DBKeys.hash_sort(user[DBKeys.HASH_KEY], 'META_STATS'))

    meta_users = ddb.batch_get_items(app_config.resource_name('analytics'), meta_keys)
    meta_users = {u[DBKeys.HASH_KEY]: u for u in meta_users}

    with open('lookalike.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['First Name', 'Last Name', 'Email', 'First Session Create Count', '1+'])
        for user in users:
            uid = user[DBKeys.HASH_KEY]
            first_session_create_count = 0
            meta_user = meta_users.get(uid)
            if meta_user is not None:
                first_session_create_count = int(meta_user.get('first_session_create_count', 0))
            name = user.get('name')
            if name is None:
                fn = user.get('given_name', '')
                ln = user.get('family_name', '')
            else:
                name = name.split()
                fn = name[0]
                if len(name) > 1:
                    ln = name[1]
            email = user['email']
            one_plus = 'yes' if first_session_create_count > 0 else 'false'
            writer.writerow([fn, ln, email, first_session_create_count, one_plus])

    tp2 = time.time()
    print(f'All done in {TerminalColors.OK_GREEN}{tp2 - tp1}{TerminalColors.END}')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    dump_users_for_lookalike()
