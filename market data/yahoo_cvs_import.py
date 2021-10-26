import csv
import time

import boto3

from clearvalue import app_config
from cvutils.store.keys import DBKeys
from cvutils.dynamodb import ddb


def load_data(reader, symbol):
    print('>>> running for', symbol)
    tp1 = time.time()
    headers = next(reader)
    # print(headers)
    batch = []
    for row in reader:
        value_date = DBKeys.history(row[0])
        item = {
            DBKeys.HASH_KEY: DBKeys.equity(symbol),
            DBKeys.SORT_KEY: value_date,
            'symbol': symbol,
            'value_date': row[0],
            'open': float(row[1]),
            'high': float(row[2]),
            'low': float(row[3]),
            'close': float(row[4]),
            'adjusted_close': float(row[5]),
            'volume': float(row[6]),
            'type': 'index'
        }
        batch.append(item)

    if len(batch) > 0:
        ddb.batch_write_items(app_config.resource_name('market'), batch)
    tp2 = time.time()
    print('*** all done for', len(batch), ' items in', tp2-tp1)


if __name__ == '__main__':
    # dev_profile = boto3.session.Session(profile_name='clearvalue-stage-sls')
    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    
    tp1 = time.time()
    for symbol in ['^DJI', '^IXIC', '^GSPC', '^VIX']:
        with open('/Users/uzix/Downloads/{}.csv'.format(symbol), 'r') as fin:
            reader = csv.reader(fin)
            load_data(reader, symbol)
    tp2 = time.time()
    print('*** all symbols done in', tp2-tp1)
