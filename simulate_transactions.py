import json
import random

from clearvalue import app_config
from cvcore.providers import yodlee
from cvcore.store import loaders
from cvutils.store.keys import DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    account_id = 'bc804a35-69db-4c62-84e6-f88f1facc98f'
    uid = 'c35fb33a-546e-4a66-adc0-1774572de7c7'

    holdings = loaders.load_account_holdings(account_id)
    symbols = None
    if holdings is not None:
        symbols = [h.get('symbol') for h in holdings if 'symbol' in h]

    if symbols is None or len(symbols) == 0:
        symbols = ['TWTR', 'MSFT', 'MDLZ', 'BAC']

    with open('users-data/dunken/transactions.json', 'r') as f:
        transactions = json.load(f)
    batch = []
    for t in transactions:
        tr = yodlee._normalize_transactions(t)
        tr[DBKeys.HASH_KEY] = account_id
        tr[DBKeys.SORT_KEY] = DBKeys.account_transaction(tr['transaction_date'], tr_time=tr['provider_item_id'])
        tr['uid'] = uid
        tr['transaction_id'] = 'P:{}:{}'.format('101', tr['provider_item_id'])
        if 'symbol' in tr:
            tr['symbol'] = random.choice(symbols)
        batch.append(tr)

    if len(batch):
        ddb.batch_write_items(app_config.resource_name('accounts'), batch)
