import json
import time

import boto3

from clearvalue import app_config
from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb


def old_dump():
    tp1 = time.time()
    found_symbols = set()
    market_table = app_config.resource_name('market')

    def _dump():
        with open('all_symbols.json', 'w') as fout:
            json.dump({'symbols': list(found_symbols)}, fout)

    # for user in loaders.iter_users():
    #     uid = user[DBKeys.HASH_KEY]
    #     accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO)
    #     user_symbols = set()
    #     for ac in accounts:
    #         if ac.get('is_high_level') is True:
    #             continue
    #
    #         holdings = loaders.load_account_holdings(ac['account_id'])
    #         for h in holdings:
    #             symbol = h.get('symbol')
    #             if symbol is None:
    #                 continue
    #
    #             user_symbols.add(symbol)
    #
    #         new_symbols = user_symbols - found_symbols
    #         symbols_to_add = set()
    #         if len(new_symbols) > 0:
    #             loaded_symbols = ddb.batch_get_items(market_table, [DBKeys.info_key(DBKeys.equity(symbol)) for symbol in new_symbols])
    #             symbols_to_add = set([sd['symbol'] for sd in loaded_symbols if sd.get('no_data') is not True])
    #             if len(symbols_to_add) > 0:
    #                 print(f'for {uid} adding {len(symbols_to_add)} -  {symbols_to_add}')
    #                 found_symbols.update(symbols_to_add)
    #                 _dump()
    #
    # _dump()
    tp2 = time.time()
    print(f'All done in {tp2 - tp1} for {len(found_symbols)} symbols')


def batch_history():
    with open('all_symbols.json', 'r') as fin:
        js = json.load(fin)

    # all_symbols = js['symbols']
    # for batch in grouper(all_symbols, 20):
    #     batch = [b for b in batch if b is not None]
    #     print(f'updating {batch}')
    #     ensure_events_for_symbols(batch, force_update=True, range='1y')


def batch_update():
    tp1 = time.time()
    market_table = app_config.resource_name('market')
    with open('all_symbols.json', 'r') as fin:
        js = json.load(fin)

    all_symbols = js['symbols']
    for symbol in all_symbols:
        print(f'Updating {symbol}')
        ddb.update_with_fields(market_table, DBKeys.info_key(DBKeys.equity(symbol)),
                               {DBKeys.GS1_HASH: 'ALL_SECURITIES', DBKeys.GS1_SORT: symbol},
                               [DBKeys.GS1_HASH, DBKeys.GS1_SORT])

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def dump2():
    market_table = app_config.resource_name('market')
    items = ddb.query(market_table,
                      IndexName='GS1-index',
                      ProjectionExpression='symbol',
                      KeyConditionExpression='GS1Hash = :HashKey',
                      ExpressionAttributeValues={
                          ':HashKey': ddb.serialize_value('ALL_SECURITIES'),
                      })
    for item in items:
        print(item)


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # old_dump()
    # batch_history()
    # batch_update()
    dump2()
