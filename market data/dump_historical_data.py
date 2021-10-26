import datetime
import json
import logging
import os

import boto3
import requests

from clearvalue import app_config
from cvcore.providers import worldtradingdata
from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb


def load_data(symbol, force=False):
    symbols_path = 'symbols/'
    base = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize=full&datatype=csv&apikey=' + app_config['alpha.vantage.key']
    dump_file = '{}{}.csv'.format(symbols_path, symbol)
    if not force and os.path.exists(dump_file):
        print('skipping', symbol)
        return

    print('Loading', symbol)
    url = base.format(symbol)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dump_file, 'wb') as fw:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    fw.write(chunk)
    else:
        print(response.text)


def dump_indexs():
    from_date = datetime.datetime(2010, 1, 1)
    # for index in app_config['securities']['indexes']:
    for index in ['^IXIC']:
        res = worldtradingdata.symbol_history(index, from_date=from_date)
        with open('indexes/{}.json'.format(index), 'w') as fout:
            json.dump(res, fout)


def load_indexes_ddb():
    for index in app_config['securities']['indexes']:
        batch = []
        with open('indexes/{}.json'.format(index), 'r') as fin:
            data = json.load(fin)
            for value_date in data:
                _data = data[value_date].copy()
                _data.update({
                    DBKeys.HASH_KEY: DBKeys.equity(index),
                    DBKeys.SORT_KEY: DBKeys.history(value_date),
                    'symbol': index,
                    'value_date': value_date,
                    'type': 'index'
                })
                batch.append(_data)
        print('For', index, 'writing', len(batch), 'data points')
        ddb.batch_write_items(app_config.resource_name('market'), batch)
    print('*** all done ***')


if __name__ == '__main__':
    dev_profile = boto3.session.Session(profile_name='clearvalue-stage-sls')
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    logging.basicConfig(level=logging.INFO)
    app_config.set_stage('staging')

    dump_indexs()
    # load_indexes_ddb()
    # dump_sp500_data()
    # load_data('VT/I')
    # for symbol in ['SPY', 'IVV', 'VOO']:
    #     load_data(symbol)
