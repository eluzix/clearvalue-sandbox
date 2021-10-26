import json

import boto3

import cvutils as utils
from cvutils import boto3_client

from clearvalue import app_config
from cvcore.providers import iexcloud
from cvutils.store.keys import DBKeys
from cvcore.store import loaders
from cvcore.model.cv_types import AccountTypes
from cvutils.dynamodb import ddb


def dump():
    recalc_accounts = []
    all_symbols = set()

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
        accounts = [a for a in accounts if a.get('is_manual') is False or a.get('is_high_level') is False]
        if len(accounts) == 0:
            continue

        recalc_accounts.extend([[uid, a['account_id']] for a in accounts])
        for account in accounts:
            holdings = loaders.load_account_holdings(account['account_id'])
            for h in holdings:
                symbol = h.get('symbol')
                if symbol is not None:
                    all_symbols.add(symbol)

    with open('/Users/uzix/Downloads/symbols-fix.json', 'w') as f:
        json.dump({'accounts': recalc_accounts, 'symbols': list(all_symbols)}, f)

    print('** all done **')

def dump_manual():
    recalc_accounts = []
    all_symbols = set()

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]
        accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
        accounts = [a for a in accounts if a.get('is_manual') is True and a.get('is_high_level', False) is False]
        if len(accounts) == 0:
            continue

        recalc_accounts.extend([[uid, a['account_id']] for a in accounts])
        # for account in accounts:
        #     holdings = loaders.load_account_holdings(account['account_id'])
        #     for h in holdings:
        #         symbol = h.get('symbol')
        #         if symbol is not None:
        #             all_symbols.add(symbol)

    with open('/Users/uzix/Downloads/manual-accounts.json', 'w') as f:
        json.dump({'accounts': recalc_accounts}, f)

    print('** all done **')


def reduce():
    with open('/Users/uzix/Downloads/symbols-fix.json', 'r') as f:
        js = json.load(f)
        js['symbols'] = list(set(js['symbols']))

    with open('/Users/uzix/Downloads/symbols-fix.json', 'w') as f:
        json.dump(js, f)

    print('** all done **')


def dump_symbols_data():
    with open('/Users/uzix/Downloads/symbols-fix.json', 'r') as f:
        js = json.load(f)
        symbols = js['symbols']

    for symbols in utils.grouper(symbols, 5, fillvalue=''):
        data = iexcloud.history_data(symbols, range='1y')
        for symbol in data:
            print(f'writing {symbol} with {len(data[symbol])} records')
            with open(f'/Users/uzix/Downloads/symbols-fix/{symbol}.json', 'w') as f:
                json.dump(data[symbol], f)
    print('*** all done ***')


def add_adjusted():
    with open('/Users/uzix/Downloads/symbols-fix.json', 'r') as f:
        js = json.load(f)
        symbols = js['symbols']

    for symbol in symbols:
        path = f'/Users/uzix/Downloads/symbols-fix/{symbol}.json'

        try:
            with open(path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            continue

        print(f'handling {symbol}')
        for item in data:
            item['adjusted_close'] = item['close']

        with open(path, 'w') as f:
            json.dump(data, f)

    print('*** all done ***')


def db_dump():
    with open('/Users/uzix/Downloads/symbols-fix.json', 'r') as f:
        js = json.load(f)
        symbols = js['symbols']

    for symbol in symbols:
        path = f'/Users/uzix/Downloads/symbols-fix/{symbol}.json'

        try:
            with open(path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            continue

        batch = []
        for item in data:
            value_date = item['date']
            batch.append({
                DBKeys.HASH_KEY: DBKeys.equity(symbol),
                DBKeys.SORT_KEY: DBKeys.history(value_date),
                'symbol': symbol,
                'value_date': value_date,
            })

        if len(batch) > 0:
            print(f'Writing {symbol} with {len(batch)} records')
            ddb.batch_write_items(app_config.resource_name('accounts'), batch)
    print('*** all done ***')


def rerun_calcs():
    with open('/Users/uzix/Downloads/manual-accounts.json', 'r') as f:
        js = json.load(f)
        accounts = js['accounts']

    # with open('/Users/uzix/Downloads/linked-accounts.json', 'r') as f:
    #     js = json.load(f)
    #     linked_accounts = [a[1] for a in js['accounts']]

    queue_url = app_config['sqs']['account.calcs.url']
    sqs = boto3_client('sqs')
    messages = []
    for account in accounts:
        # if account[1] in linked_accounts:
        #     continue
        msg = {'uid': account[0], 'account_id': account[1], 'action': 'manual-sp-recalc'}
        messages.append(json.dumps(msg))

    for entries in utils.grouper(messages, 5, fillvalue=''):
        kv = {
            'QueueUrl': queue_url,
            'Entries': [],
        }
        for entry in entries:
            kv['Entries'].append({
                'Id': utils.random_str(32),
                'MessageBody': entry
            })
        print(kv)
        ret = sqs.send_message_batch(**kv)
        print(ret)

    print(f'*** all done {len(messages)} ***')


def dump_linked_only():
    with open('/Users/uzix/Downloads/symbols-fix.json', 'r') as f:
        js = json.load(f)
        accounts = js['accounts']

    linked_accounts = []
    for account in accounts:
        dba = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(account[0], account[1]))
        if dba.get('is_manual') is False:
            linked_accounts.append(account)

    with open('/Users/uzix/Downloads/linked-accounts.json', 'w') as f:
        json.dump({'accounts': linked_accounts}, f)


def rerun_users_calcs():
    with open('/Users/uzix/Downloads/linked-accounts.json', 'r') as f:
        js = json.load(f)
        accounts = js['accounts']

    queue_url = app_config['sqs']['user.calcs.url']
    sqs = boto3_client('sqs')
    messages = []
    rf = utils.date_to_str(utils.today())
    users = set()
    for account in accounts:
        if account[0] in users:
            continue

        users.add(account[0])
        msg = {'uid': account[0], 'run_for': rf}
        messages.append(json.dumps(msg))

    for entries in utils.grouper(messages, 5, fillvalue=''):
        kv = {
            'QueueUrl': queue_url,
            'Entries': [],
        }
        for entry in entries:
            kv['Entries'].append({
                'Id': utils.random_str(32),
                'MessageBody': entry
            })
        print(kv)
        ret = sqs.send_message_batch(**kv)
        print(ret)

    print(f'*** all done for {len(messages)} users ***')


def rerun_linked_calcs():
    with open('/Users/uzix/Downloads/linked-accounts.json', 'r') as f:
        js = json.load(f)
        accounts = js['accounts']

    queue_url = app_config['sqs']['account.calcs.url']
    sqs = boto3_client('sqs')
    messages = []
    for account in accounts:
        msg = {'uid': account[0], 'account_id': account[1], 'action': 'linked-sp-recalc'}
        messages.append(json.dumps(msg))

    for entries in utils.grouper(messages, 5, fillvalue=''):
        kv = {
            'QueueUrl': queue_url,
            'Entries': [],
        }
        for entry in entries:
            if entry == '':
                continue
                
            kv['Entries'].append({
                'Id': utils.random_str(32),
                'MessageBody': entry
            })
        print(kv)
        # ret = sqs.send_message_batch(**kv)
        # print(ret)

    print('*** all done ***')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    rerun_calcs()
    # dump_manual()