import json
import os
import time
from os import path

import boto3
import requests

from clearvalue import app_config
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.providers import coingecko
from clearvalue.lib.search import elastic
from clearvalue.lib.store import DBKeys


def dump_all():
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'w') as f:
        all_coins = coingecko.all_coins()
        coins = {
            c['symbol'].upper(): {
                'symbol': c['symbol'].upper(),
                'id': c['id'],
                'name': c['name'],
            } for c in all_coins
        }
        json.dump(coins, f)


def enrich_coins():
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        coins = json.load(f)

    i = 1
    batch = 1
    for symbol in coins:
        if coins[symbol].get('icon_url') is not None:
            continue

        _id = coins[symbol]['id']
        try:
            res = requests.get(f"https://api.coingecko.com/api/v3/coins/{_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false&sparkline=false").json()
            # desc = res.get('description', {}).get('en')
            # if desc is not None:
            #     coins[symbol]['description'] = desc
            logo = res.get('image', {}).get('large')
            if logo is None:
                logo = res.get('image', {}).get('small')
            if logo is not None:
                coins[symbol]['icon_url'] = logo

            with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'w') as f:
                json.dump(coins, f)

            i += 1
            if i >= 50:
                print(f"sleeping for batch {batch}")
                try:
                    time.sleep(31)
                except:
                    pass
                finally:
                    i = 1
                    batch += 1
        except Exception as e:
            print('in except block waiting for 31s', e)
            try:
                time.sleep(31)
            except:
                pass
    print('writing final time...')
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'w') as f:
        json.dump(coins, f)

    print('all done')


def dump_missing_history():
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        all_coins = json.load(f)

    for symbol in all_coins:
        coin = all_coins[symbol]
        if path.exists(f'data/{symbol}.json'):
            print(f"skipping {symbol}")
            continue

        _id = coin['id']
        try:
            res = requests.get(f"https://api.coingecko.com/api/v3/coins/{_id}/market_chart?vs_currency=usd&days=max&interval=daily").json()
            all_data = []
            for price in res['prices']:
                date = utils.date_from_timestamp(price[0] / 1000)
                value = price[1]
                all_data.append([utils.date_to_str(date), value])

            print(f'Writing {symbol}.json')
            with open(f'data/{symbol}.json', 'w') as f:
                json.dump({'data': all_data}, f)

            try:
                time.sleep(2)
            except:
                pass

        except Exception as e:
            print('in except block waiting for 31s', e)
            try:
                time.sleep(31)
            except:
                pass

    print('* all done *')


def dump_symbol_history(symbol, override=False):
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        all_coins = json.load(f)

    coin = all_coins[symbol]
    if not override and path.exists(f'data/{symbol}.json'):
        print(f"skipping {symbol} - already exists")
        return

    _id = coin['id']
    try:
        res = requests.get(f"https://api.coingecko.com/api/v3/coins/{_id}/market_chart?vs_currency=usd&days=max&interval=daily").json()
        all_data = []
        for price in res['prices']:
            date = utils.date_from_timestamp(price[0] / 1000)
            value = price[1]
            all_data.append([utils.date_to_str(date), value])

        print(f'Writing {symbol}.json')
        with open(f'data/{symbol}.json', 'w') as f:
            json.dump({'data': all_data}, f)

    except Exception as e:
        print('Error in except block', e)

    print('* all done *')


def db_dump(basepath='data/'):
    tp1 = time.time()
    for entry in os.listdir(basepath):
        if os.path.isfile(os.path.join(basepath, entry)):
            with open(os.path.join(basepath, entry), 'r') as fin:
                if not entry.endswith('.json'):
                    continue

                symbol = entry.replace('.json', '')
                batch = []
                data = json.loads(fin.read())['data']
                if len(data) < 2:
                    continue

                dates_found = set()
                for point in data:
                    if point[0] in dates_found:
                        continue

                    coin = {
                        DBKeys.HASH_KEY: DBKeys.crypto(symbol),
                        DBKeys.SORT_KEY: DBKeys.crypto_history(point[0]),
                        'value_date': point[0],
                        'symbol': symbol,
                        'value': point[1],
                        'rate': point[1],
                    }
                    batch.append(coin)
                    dates_found.add(point[0])
                if len(batch) > 0:
                    print(f'Writing {symbol} with {len(batch)} points')
                    ddb.batch_write_items(app_config.resource_name('market'), batch)

    tp2 = time.time()
    print(f'*** All done in {tp2 - tp1} seconds')


def db_dump_symbol(symbol):
    tp1 = time.time()
    basepath = 'data/'
    with open(os.path.join(basepath, f'{symbol}.json'), 'r') as fin:
        batch = []
        data = json.loads(fin.read())['data']
        if len(data) < 2:
            return

        dates_found = set()
        for point in data:
            if point[0] in dates_found:
                continue

            coin = {
                DBKeys.HASH_KEY: DBKeys.crypto(symbol),
                DBKeys.SORT_KEY: DBKeys.crypto_history(point[0]),
                'value_date': point[0],
                'symbol': symbol,
                'value': point[1],
                'rate': point[1],
            }
            batch.append(coin)
            dates_found.add(point[0])
        if len(batch) > 0:
            print(f'Writing {symbol} with {len(batch)} points')
            ddb.batch_write_items(app_config.resource_name('market'), batch)

    tp2 = time.time()
    print(f'*** All done in {tp2 - tp1} seconds')


def dump_to_elastic(profile=None):
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        all_coins = json.load(f)

        batch = []
        for symbol in all_coins:
            coin = all_coins[symbol]
            doc = {
                'suggest': [coin['name'].lower(), symbol.lower()],
                'id': symbol.upper(),
                'internal_id': coin['id'],
                'name': coin['name'],
                'symbol': symbol,
            }
            icon = coin.get('icon_url')
            if icon is not None:
                doc['icon'] = icon

            batch.append(doc)
    elastic.index_docs('crypto', batch, boto_session=profile)


def dump_coin_elastic(symbol, profile=None):
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        all_coins = json.load(f)

        batch = []
        coin = all_coins[symbol]
        doc = {
            'suggest': [coin['name'].lower(), symbol.lower()],
            'id': symbol.upper(),
            'internal_id': coin['id'],
            'name': coin['name'],
            'symbol': symbol,
        }
        icon = coin.get('icon_url')
        if icon is not None:
            doc['icon'] = icon

        batch.append(doc)
    elastic.index_docs('crypto', batch, boto_session=profile)


def add_coin(symbol, override=False, all_coins=None):
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        existing_coins = json.load(f)

    if not override and symbol in existing_coins.keys():
        print(f'{symbol} already in crypto-coins-v3.json')
        return

    if all_coins is None:
        all_coins = coingecko.all_coins()
    for coin in all_coins:
        if coin['symbol'].lower() == symbol.lower() or coin['id'].lower() == symbol.lower():
            _id = coin['id']
            c = {
                'symbol': coin['symbol'].upper(),
                'id': _id,
                'name': coin['name'],
            }
            res = requests.get(f"https://api.coingecko.com/api/v3/coins/{_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false&sparkline=false").json()
            # desc = res.get('description', {}).get('en')
            # if desc is not None:
            #     coins[symbol]['description'] = desc
            logo = res.get('image', {}).get('large')
            if logo is None:
                logo = res.get('image', {}).get('small')
            if logo is not None:
                c['icon_url'] = logo

            existing_coins[c['symbol']] = c

            print(f'Adding {c["symbol"]} to crypto-coins-v3.json')
            with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'w') as f:
                json.dump(existing_coins, f)
            break
    print('*** All Done ***')


def fix_history():
    with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
        all_coins = json.load(f)

    for symbol in all_coins:
        coin = all_coins[symbol]
        if path.exists(f'data-fix/{symbol}.json'):
            print(f"skipping {symbol}")
            continue

        _id = coin['id']
        try:
            res = requests.get(f"https://api.coingecko.com/api/v3/coins/{_id}/market_chart?vs_currency=usd&days=10&interval=daily").json()
            all_data = []
            for price in res.get('prices', []):
                date = utils.date_from_timestamp(price[0] / 1000)
                value = price[1]
                all_data.append([utils.date_to_str(date), value])

            print(f'Writing {symbol}.json')
            with open(f'data-fix/{symbol}.json', 'w') as f:
                json.dump({'data': all_data}, f)

            try:
                time.sleep(2)
            except:
                pass

        except Exception as e:
            print('in except block waiting for 31s', e)
            try:
                time.sleep(31)
            except:
                pass

    print('* all done *')


if __name__ == '__main__':
    profile = None
    # profile = boto3.session.Session(profile_name='clearvalue-sls')
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')
    profile = boto3.session.Session(profile_name='clearvalue-stage-sls')
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')

    # dump_all()
    # enrich_coins()
    # dump_missing_history()
    # db_dump()
    dump_to_elastic(profile)

    # 2018-01-23

    # add_coin('XDC')
    # dump_symbol_history('XDC', override=True)
    # db_dump_symbol('XDC')
    # dump_coin_elastic('XDC', profile=profile)

    # dump_missing_history()
    # db_dump_symbol('STX')
    # dump_to_elastic(profile)

    # fix_history()
    # db_dump('data-fix/')

    # with open('../../../clearvalue-api/resources/crypto-coins-v3.json', 'r') as f:
    #     coins = json.load(f)
    # print(len(coins))

    # add_coin('KAREN')
    # all_coins = coingecko.all_coins()
    # coins = {'KAREN', 'RMOON', 'COLD', 'TGDY', 'SAFEMOONCASH', 'SAFECOOKIE', 'BIDCOM', 'HAPPY', 'SAFEMUSK', 'Hamtaro', 'MNTT', 'HOME', 'MetaMoon', 'SBYTE', 'ORANGE', '100x', 'SHILD', 'PHX', 'GRIMEX', 'SXI', 'HyMETEOR', 'AQUAGOAT', 'EnergyX', 'ICEBRK', 'ULTRA', 'BB', 'PIT', 'FECLIPSE', 'GNT', 'SWASS', 'ERTH', 'PEKC', 'MOONPIRATE', 'SAFEORBIT', 'FULLSEND', 'HMNG', 'KODURO', 'WSG', 'KAWAII', 'LTN', 'SAFEETH', 'STORY', 'SAFESPACE', 'Cake', 'GEMS', 'MDAO', 'MOOCHII', 'ORION', 'CHARIX', 'BLOSM', 'NFTBOX', 'LTRBT', 'GAMESAFE', 'HEAT', '$BOOB', 'SLAM', 'FootballStars', 'ASS', 'WAIV', 'LUNAR', 'TASTE', 'SFMS', 'CAROM', 'HUNGRY', 'FSAFE', 'NFTART', 'ENVIRO', 'GLXM', 'CLU', 'X-Token', 'LTMS', 'GDT', 'CERBERUS', 'BOOZE', 'SAFEROCKET', 'PinkM'}
    # coins = {'EnergyX'}
    # for coin in coins:
    #     coin = coin.upper()
    #     try:
    #         dump_symbol_history(coin)
    #         db_dump_symbol(coin)
    #         dump_coin_elastic(coin, profile=profile)
    #         # time.sleep(5)
    #     except Exception as e:
    #         print(f'Failed to add {coin}, error: {e}')


