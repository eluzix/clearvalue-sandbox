import json

import boto3

from clearvalue import app_config
from clearvalue.lib import crypto_utils

base_dir = '/Users/uzix/Dev/Clear Value/secrets/SSL'


def encrypt():
    encrypted_key, plain_key = crypto_utils.get_data_keys()

    with open('{}/app.claritus.io.private.key'.format(base_dir), 'r') as f:
        data = f.read()

    enc_data = crypto_utils.encrypt(data, plain_key=plain_key)

    with open('{}/app.claritus.io.private.key.json'.format(base_dir), 'w') as f:
        json.dump({'data': enc_data, 'key': encrypted_key}, f)

    print('Done.')


def decrypt():
    with open('{}/app.claritus.io.private.key.json'.format(base_dir), 'r') as f:
        js = json.load(f)

    enc_data = js['data']
    encrypted_key = js['key']

    decrypted_private_key = crypto_utils.decrypt(enc_data, encrypted_key=encrypted_key)
    print(decrypted_private_key)


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-uzi')
    app_config.set_stage('prod')

    encrypt()
    # decrypt()
