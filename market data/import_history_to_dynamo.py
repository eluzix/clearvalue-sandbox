import csv
import time

from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb


def dump_batch(batch):
    request_items = {
        'cv-market-dev': batch
    }
    res = ddb.get_connection().batch_write_item(RequestItems=request_items)
    # todo: handle UnProcessedItems with expo backoff


def load_symbol(symbol, batch_rows, upto_date):
    batch_count = 1
    records = 0
    symbols_path = 'symbols/'
    load_file = '{}{}.csv'.format(symbols_path, symbol)

    with open(load_file, 'r') as fr:
        print('Loading symbol', symbol)
        sym_reader = csv.reader(fr)
        _ = next(sym_reader)

        for row in sym_reader:
            if upto_date is not None and row[0] < upto_date:
                print('breaking for', row)
                break

            # timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient
            batch_rows.append({'PutRequest': {
                'Item': ddb.type_serialize({
                    DBKeys.HASH_KEY: DBKeys.equity(symbol),
                    DBKeys.SORT_KEY: DBKeys.history(row[0]),
                    'symbol': symbol,
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'adjusted_close': float(row[5]),
                    'volume': int(row[6]),
                    'dividend_amount': float(row[7]),
                    'split_coefficient': float(row[8])

                })
            }})
            records += 1

            if len(batch_rows) == 25:
                print('dumping batch', batch_count)
                dump_batch(batch_rows)
                batch_rows.clear()
                batch_count += 1

    return batch_count, records


def import_all(upto_date=None, load_symbols=None):
    if load_symbols is None:
        load_symbols = []

    tp1 = time.time()
    symbols_path = 'symbols/'
    with open('SP500.csv', 'r') as f:
        reader = csv.reader(f)
        # clean headers
        _ = next(reader)

        batch_rows = []
        batch_count = 1
        records = 0

        for symbol in reader:
            _batch_count, _records = load_symbol(symbol[0], batch_rows, upto_date)
            batch_count += _batch_count
            records += _records

    if len(batch_rows) > 0:
        print('dumping final batch', batch_count)
        dump_batch(batch_rows)
        batch_rows.clear()

    tp2 = time.time()
    print('* all done in', (tp2 - tp1), 'seconds, loaded', records, 'items in', batch_count, 'batches')


if __name__ == '__main__':
    # import_all('2019-05-01')
    batch_rows = []
    batch_count = 1
    records = 0

    for symbol in ['V', 'VT', 'VTI', 'SPY', 'IVV', 'VOO']:
        load_symbol(symbol, batch_rows, '2019-01-01')

    if len(batch_rows) > 0:
        print('dumping final batch', batch_count)
        dump_batch(batch_rows)
        batch_rows.clear()

    print('* All done *')
