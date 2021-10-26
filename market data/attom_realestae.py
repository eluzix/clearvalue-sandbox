import json
import time

import boto3
import requests

from clearvalue import app_config
from cvcore.store.keys import DBKeys
from cvcore.store import loaders
from cvcore.model.cv_types import AccountTypes
from utils.users_report import ValueEncoder


def dump_address():
    tp1 = time.time()
    all_items = []
    all_addresses = set()

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        properties = loaders.load_user_accounts(uid, account_type=AccountTypes.REAL_ESTATE, ProjectionExpression='address, address_info')
        for item in properties:
            full_address = None
            address = item.get('address')
            address_info = item.get('address_info')

            if address is not None:
                full_address = address.get('description')
            elif address_info is not None:
                full_address = address.get('formatted_address')

            if full_address is None:
                continue

            if full_address in all_addresses:
                continue

            print(f'found: {full_address}')
            all_addresses.add(full_address)
            all_items.append({
                'full_address': full_address,
                'address': address,
                'address_info': address_info,
            })

    with open('realestate-address.json', 'w') as f:
        json.dump(all_items, f, cls=ValueEncoder)
    tp2 = time.time()
    print(f'*** all done in {tp2 - tp1}, wrote {len(all_items)} addresses ***')


def attom_search_property(full_address):
    address_split = full_address.split(',')

    address1 = address_split[0]
    state = address_split[2].split()[0]
    address2 = ', '.join([address_split[1], state])

    resp = requests.get('https://api.gateway.attomdata.com/propertyapi/v1.0.0/property/basicprofile', params={
        'address1': address1,
        'address2': address2
    }, headers={
        'apikey': '2f6e9337b6cd9929485e901e214f842a',
        'Accept': 'application/json'
    }).json()

    if resp['status']['total'] > 0:
        return resp['property']

    return None


def process_attom_address():
    with open('realestate-address.json', 'r') as f:
        js = json.load(f)

    count = 0
    for address in js:
        country_code = address['address'].get('country_code')
        if country_code is None or country_code.lower() != 'us':
            continue

        if 'attom' in address:
            continue

        count += 1
        full_address = address["full_address"]
        try:
            print(f'searching for {full_address}')
            search_result = attom_search_property(full_address)
            address['attom'] = search_result
        except Exception as e:
            print(f"for {full_address} error: {e}")

    with open('realestate-address.json', 'w') as f:
        json.dump(js, f)

    print(f'Total US addresses: {count}')


def simplyrets_search_property(address):
    pass

def process_simplyrets_address():
    with open('realestate-address.json', 'r') as f:
        js = json.load(f)

    count = 0
    for address in js:
        country_code = address['address'].get('country_code')
        if country_code is None or country_code.lower() != 'us':
            continue

        if 'simplyrets' in address:
            continue

        count += 1
        full_address = address["full_address"]
        try:
            print(f'searching for {address["address"]}')
            # search_result = attom_search_property(full_address)
            # address['attom'] = search_result
        except Exception as e:
            print(f"for {full_address} error: {e}")

    with open('realestate-address.json', 'w') as f:
        json.dump(js, f)

    print(f'Total US addresses: {count}')


def stats():
    with open('realestate-address.json', 'r') as f:
        js = json.load(f)

    count = 0
    with_attom = 0
    for address in js:
        country_code = address['address'].get('country_code')
        if country_code is None or country_code.lower() != 'us':
            continue

        count += 1
        if address.get('attom') is not None:
            with_attom += 1
            print(address.get('attom'))

    print(f'Total US addresses: {count}, with attom data {with_attom} == {with_attom/count}%')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # dump_address()
    # process_attom_address()
    # print(attom_search_property("10440 Queens Blvd, Forest Hills, NY 11375, USA"))
    process_simplyrets_address()
