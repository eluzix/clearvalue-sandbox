import boto3
import requests

from clearvalue import app_config
from cvutils.store.keys import DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    api_token = app_config['providers']['iexcloud']['key']
    base_url = f'{app_config["providers"]["iexcloud"]["base_url"]}/stock/AMZN/dividends/3m'
    params = {
        'token': api_token,
    }
    resp = requests.get(base_url, params=params).json()

    update_func = lambda tp, symbol, event_date: {DBKeys.HASH_KEY: f'EVNT:{event_date}',
                                                  DBKeys.SORT_KEY: f'{symbol}:{tp}',
                                                  DBKeys.GS1_HASH: f'SMBL:EVNT:{symbol}',
                                                  DBKeys.GS1_SORT: f'{event_date}:{tp}',
                                                  'symbol': symbol,
                                                  'event_type': tp}
    batch = []
    keys = set()
    for event in resp:
        symbol = event.get('symbol')
        if symbol is None:
            continue

        event_date = event.get('paymentDate')
        if event_date is None or event_date == '0000-00-00':
            event_date = event.get('recordDate')
        if event_date is None or event_date == '0000-00-00':
            event_date = event.get('exDate')
        if event_date is None or event_date == '0000-00-00':
            continue

        if event_date is None:
            continue

        k = f'dividend, {symbol}, {event_date}'
        if k in keys:
            continue

        keys.add(k)
        event.update(update_func('dividend', symbol, event_date))
        batch.append(event)

    print(batch)

    if len(batch) > 0:
        print(f'Writing {len(batch)} records')
        ddb.batch_write_items(app_config.resource_name('market'), batch)
    print('*** All Done ***')
