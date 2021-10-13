import datetime
import json
import logging
import os
import shutil
import time

import boto3
import requests
from bs4 import BeautifulSoup

from clearvalue import app_config
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import DBKeys


def dump_data():
    with open('crypto.html', 'r') as f:
        soup = BeautifulSoup(f.read(), features="html.parser")
        table = soup.find('table')
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        all_coins = []
        for row in rows:
            cols = row.find_all('td')
            logo = cols[1].find('img', {'class': 'cmc-static-icon'}).get('src').replace('32x32', '128x128')

            coin = {
                'name': cols[1].text.strip(),
                'symbol': cols[2].text.strip(),
                'logo': logo
            }

            r = requests.get(logo, stream=True)
            if r.status_code == 200:
                with open('coins/{}.png'.format(coin['symbol']), 'wb') as fout:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, fout)

            # print(coin)
            all_coins.append(coin)

        with open('coins/coins.json', 'w') as fout:
            fout.write(json.dumps(all_coins))


def sync_lists():
    final_list = {}
    # with open('coins/coins.json', 'r') as f1:
    #     original = json.loads(f1.read())
    #     original = {c['symbol']: {'symbol': c['symbol'], 'name': c['name']} for c in original}

    url = 'http://api.coinlayer.com/list?access_key={}'.format(app_config['coinlayer.key'])
    res = requests.get(url).json()
    for symbol in res['crypto']:
        coin = res['crypto'][symbol]
        final_list[symbol] = {'symbol': symbol, 'name': coin['name'], 'description': coin['name_full'], 'icon_url': coin['icon_url']}

    # with open('digital_currency_list.csv', 'r') as f2:
    #     reader = csv.reader(f2)
    #     reader.__next__()
    #     for row in reader:
    #         if row[0] in original:
    #             final_list.append(original[row[0]])

    with open('cv-coins-list.json', 'w') as fout:
        fout.write(json.dumps(final_list))


def load_history_data(from_date, to_date):
    # https://api.coinlayer.com/timeframe
    # ? access_key = YOUR_ACCESS_KEY
    # & start_date = 2018-04-01
    # & end_date = 2018-04-30
    # & symbols = BTC,ETH

    params = {
        'access_key': app_config['coinlayer.key'],
        'start_date': from_date,
        'end_date': to_date,
        'expand': '1'
    }

    res = requests.get('https://api.coinlayer.com/timeframe', params=params).json()
    with open('coinlayer/hisotry-{}-{}.json'.format(from_date, to_date), 'w') as fout:
        fout.write(json.dumps(res))


def coinlayer_dump():
    tp1 = time.time()

    start_date = datetime.datetime(2011, 1, 1)

    for i in range(9):
        end_date = start_date + datetime.timedelta(days=365)
        # print('running from', start_date, 'to', end_date)
        load_history_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        start_date = end_date

    start_date = datetime.datetime(2020, 1, 1)
    end_date = utils.today()
    print('running from', start_date, 'to', end_date)
    load_history_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    tp2 = time.time()
    print('*** All done in', tp2 - tp1, 'seconds')


def db_dump():
    tp1 = time.time()
    basepath = 'coinlayer/'
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            with open(os.path.join(basepath, entry), 'r') as fin:
                print('Processing', entry)
                all_data = []
                rates = json.loads(fin.read())['rates']
                for dt in rates:
                    for symbol in rates[dt]:
                        coin = {k: rates[dt][symbol][k] for k in rates[dt][symbol] if rates[dt][symbol][k] is not None}
                        coin['symbol'] = symbol
                        coin['value_date'] = dt
                        coin[DBKeys.HASH_KEY] = DBKeys.crypto(symbol)
                        coin[DBKeys.SORT_KEY] = DBKeys.crypto_history(dt)

                        all_data.append(coin)
                ddb.batch_write_items(app_config.resource_name('market'), all_data)

    tp2 = time.time()
    print('*** All done in', tp2 - tp1, 'seconds')


if __name__ == '__main__':
    dev_profile = boto3.session.Session(profile_name='clearvalue-stage-sls')
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    logging.basicConfig(level=logging.INFO)
    app_config.set_stage('staging')
    # coinlayer_dump()
    db_dump()
