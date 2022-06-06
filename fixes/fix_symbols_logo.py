import boto3

import cvutils
from clearvalue import app_config
from cvcore.store import loaders
from cvutils.config import get_app_config
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    market_table = app_config.resource_name('market')
    items = ddb.query(market_table,
                      IndexName='GS1-index',
                      ProjectionExpression='SortKey, symbol,logo',
                      KeyConditionExpression='GS1Hash = :HashKey',
                      ExpressionAttributeValues={
                          ':HashKey': ddb.serialize_value('ALL_SECURITIES'),
                      })

    all_symbols = set()
    for item in items:
        if 'logo' in item:
            continue
        if item['SortKey'] == 'INFO':
            continue

        all_symbols.add(item['symbol'])

    print(f'total symbols: {len(all_symbols)}')
    print(all_symbols)
    # count = 1
    # for chunk in cvutils.grouper(list(all_symbols), 10):
    #     symbols = [c for c in chunk if c is not None]
    #     print(f'Executing run {count}')
    #     loaders.load_security_info(symbols, force_reload=True)
    #     count += 1

    print(f'All done')
