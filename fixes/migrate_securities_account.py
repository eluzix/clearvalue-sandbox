import json

import boto3

import cvutils as utils
from cvutils import boto3_client

from clearvalue import app_config
from clearvalue.lib.store import loaders, DBKeys
from clearvalue.model.cv_types import AccountTypes, InvestmentTransactionType
from cvutils.dynamodb import ddb


def handle_user_accounts(uid):
    table_name = app_config.resource_name('accounts')
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
    accounts = [a for a in accounts if a.get('is_manual') is True and a.get('is_high_level') is not True]

    if len(accounts) > 0:
        print(f'for {uid} loading {len(accounts)} accounts')

    for account in accounts:
        batch = []
        delete_batch = []
        account_id = account['account_id']
        holdings = loaders.load_account_holdings(account_id)
        transactions = loaders.load_account_transactions(account_id)
        found_symbols = {}
        for t in transactions:
            transaction_type = t.get('transaction_type')
            quantity = float(t.get('quantity', 0))
            symbol = t.get('symbol')
            symbol_trans = found_symbols.get(symbol)
            if symbol_trans is None:
                symbol_trans = 0

            delete_batch.append(DBKeys.hash_sort(t[DBKeys.HASH_KEY], t[DBKeys.SORT_KEY]))
            if transaction_type == 'update':
                t[DBKeys.SORT_KEY] = f'{t[DBKeys.SORT_KEY]}:{symbol}'
                t['transaction_type'] = 'buy'
                symbol_trans += quantity
                batch.append(t)
                
            elif transaction_type == 'buy':
                symbol_trans += quantity
            elif transaction_type == 'sell':
                symbol_trans -= quantity

            found_symbols[symbol] = symbol_trans
        for h in holdings:
            symbol = h.get('symbol')
            if symbol is None:
                continue

            existing_quantity = h.get('quantity')
            if existing_quantity is None:
                continue
            existing_quantity = float(existing_quantity)
            trans_quantity = found_symbols.get(symbol, 0)
            quantity_gap = existing_quantity - trans_quantity
            if quantity_gap != 0:
                transaction_date = utils.date_from_timestamp(h['created_at'])
                current_value = loaders.load_securities([symbol], for_date=transaction_date).get(symbol)
                if current_value is not None:
                    current_value = current_value.get('adjusted_close', current_value.get('close'))
                if current_value is None:
                    current_value = float(h['value']) / existing_quantity

                if quantity_gap < 0:
                    transaction_type = 'sell'
                else:
                    transaction_type = 'buy'

                trans_value = float(quantity_gap * current_value)
                print(f'[{account_id}] for {symbol} creating {transaction_type} transaction with {quantity_gap} units at {transaction_date} for value {trans_value}')
                batch.append({
                    DBKeys.HASH_KEY: account_id,
                    DBKeys.SORT_KEY: f"{DBKeys.account_transaction(transaction_date, int(h['created_at']))}:{symbol}",
                    'uid': uid,
                    'transaction_id': utils.generate_id(),
                    'transaction_date': utils.date_to_str(transaction_date),
                    'transaction_type': transaction_type,
                    'holding_id': h['holding_id'],
                    'value': trans_value,
                    'quantity': quantity_gap,
                    'symbol': symbol})

        if len(batch) > 0:
            print(f'For {uid}/{account_id} writing {len(batch)} records and {len(delete_batch)} deletes')
            # print(batch)
            # print(delete_batch)
            ddb.batch_write_items(table_name, batch)
            if len(delete_batch) > 0:
                ddb.batch_delete_items(table_name, delete_batch)

            queue_url = app_config['sqs']['account.calcs.url']
            msg = {'uid': uid, 'account_id': account_id, 'action': 'manual-sp-recalc'}
            boto3_client('sqs').send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))


if __name__ == '__main__':
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    demo_user_uid = '4aaa981b-004b-4c39-a743-979ee062ddee'
    andy_uid = 'e446541c-4ea5-430b-ad1d-93ac2121ebc5'

    # handle_user_accounts(andy_uid)

    for user in loaders.iter_users():
        if user[DBKeys.HASH_KEY] != demo_user_uid:
            if user[DBKeys.HASH_KEY] == andy_uid:
                print('Running for andy!!!!!!!')
            handle_user_accounts(user[DBKeys.HASH_KEY])
