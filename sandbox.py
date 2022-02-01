import datetime
import time

import boto3
import pytz

import cvutils
from clearvalue import app_config
from cvcore.store import DBKeys, loaders
from cvutils import elastic
from cvutils.config import get_app_config
from cvutils.dynamodb import ddb


def day_of_interest(dt: datetime.datetime, day_of_interest: int) -> int:
    first_of_month = datetime.datetime(dt.year, dt.month, 1, tzinfo=pytz.utc)
    interest_date = first_of_month + datetime.timedelta(days=day_of_interest - 1)

    if dt.month == interest_date.month:
        return day_of_interest

    # if interest day isn't in the month return last day of month
    last_of_month = interest_date - datetime.timedelta(days=interest_date.day)
    return last_of_month.day


def w():
    all_errors = {}
    error_type = 'action:{action_name}'
    existing_error = all_errors.get(error_type)
    if existing_error is None:
        all_errors[error_type] = 'only 1 error message here'
    else:
        all_errors[error_type] = 'general error message here'


def timing_test1():
    uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'
    account_id = 'ff12268d-1815-40d7-9e7f-617a2c14cf41'
    account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    start_date = '2020-10-21'
    end_date = cvutils.date_to_str(cvutils.today())

    tp1 = time.time()
    graph_data = loaders.load_account_graph_data(account, 'custom', start_date, end_date)
    tp2 = time.time()
    print(f'[TEST 1] Loaded {len(graph_data)} in {tp2 - tp1}')


def timing_test2():
    uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'
    account_id = 'ff12268d-1815-40d7-9e7f-617a2c14cf41'
    account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    start_date = '2020-10-21'
    end_date = cvutils.date_to_str(cvutils.today())

    tp1 = time.time()
    cur_date = cvutils.date_from_str(start_date) + datetime.timedelta(days=1)
    keys = [DBKeys.hash_sort(account_id, DBKeys.account_time_point(start_date))]
    run_till = cvutils.date_from_str(end_date)
    while cur_date < run_till:
        if cur_date.weekday() == 6:
            keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(cur_date)))
        # keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(cur_date)))
        cur_date = cur_date + datetime.timedelta(days=1)
    keys.append(DBKeys.hash_sort(account_id, DBKeys.account_time_point(end_date)))

    # graph_data = loaders.load_account_graph_data(account, 'custom', start_date, end_date)
    graph_data = ddb.batch_get_items(app_config.resource_name('accounts'), keys)

    tp2 = time.time()
    print(f'[TEST 2] Loaded {len(graph_data)} in {tp2 - tp1}')


def query():
    accounts_table_name = get_app_config().resource_name('accounts')
    account_id = '4df35a2c-0186-497d-972b-3b1a045fad8a'
    start_date = '2000-01-01'
    end_date = '2000-01-03'

    if isinstance(start_date, str):
        start_date = cvutils.date_from_str(start_date)
    if isinstance(end_date, str):
        end_date = cvutils.date_from_str(end_date)

    items = ddb.query(accounts_table_name,
                      KeyConditionExpression='HashKey = :HashKey AND SortKey BETWEEN :start_date AND :end_date',
                      ExpressionAttributeValues={
                          ':HashKey': ddb.serialize_value(account_id),
                          ':start_date': ddb.serialize_value(DBKeys.account_time_point(start_date)),
                          ':end_date': ddb.serialize_value(DBKeys.account_time_point(end_date)),
                      })
    print(items)


def join_utm(boto_session):
    client = elastic.client(boto_session=boto_session)
    body = {
        'query': {
            'bool': {
                "must": [
                    {"match": {"log_name": "client-log"}},
                    {"match": {"event": "user created"}},
                    {"match": {"category": "signup"}},
                ],
            },
        },
        'size': 300
    }

    results = client.search(body=body, index='app-logs*')
    hits = results['hits']

    for hit in hits['hits']:
        source = hit['_source']
        history = source.get('utm_history')
        if history is not None:
            print(source['uid'], history)


if __name__ == '__main__':
    # boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    # timing_test1()
    # timing_test2()
    # print(ddb.batch_get_items(app_config.resource_name('accounts'), [DBKeys.user_account('sd', '12')]))
    # query()

    # db_account = loaders.load_user_account('5bbc7a83-fa1a-42d3-a908-b5cebb9a7e09', '1d8dc5aa-8c64-4c3d-99d4-0624d9fe61da')
    # print(db_account['value'])
    # update_account_holdings_latest_data(db_account)
    # print(db_account['value'])

    # ddb.collect_query_data = True
    # ret = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key('f417d7b5-cb66-4ef1-a36a-9c6806d0af0f'))
    # print(ret)

    # join_utm(boto_session)
    a = [{'HashKey': 'CRY:CV-BTM-BYTOM', 'SortKey': 'INFO', 'symbol': 'BTM', 'provider_id': 'bytom', 'name': 'Bytom',
          'provider_data': {'id': 'bytom', 'symbol': 'btm', 'name': 'Bytom', 'asset_platform_id': None, 'platforms': {'': '0xcb97e65f07da24d46bcdd078ebebd7c6e6e3d750'}, 'block_time_in_minutes': 0,
                            'hashing_algorithm': None, 'categories': ['Cosmos Ecosystem', 'Polygon Ecosystem', 'Smart Contract Platform'], 'public_notice': None, 'additional_notices': [],
                            'description': {
                                'en': 'Bytom is a blockchain protocol for financial and digital asset applications. Using the Bytom protocol, individuals and enterprises alike can register and exchange not just digital assets (i.e. Bitcoin) but traditional assets as well (i.e. securities, bonds, or even intelligence data). Bytom’s mission is “to bridge the atomic [physical] world and the byte [digital] world, to build a decentralized network where various byte assets and atomic assets could be registered and exchanged.”\r\n\r\nBytom is an interactive protocol of multiple byte assets, to give it the proper title. Heterogeneous byte-assets (indigenous digital currency, digital assets) that operate in different forms on the Bytom Blockchain and atomic assets (warrants, securities, dividends, bonds, intelligence information, forecasting information and other information that exist in the physical world) can be registered, exchanged, gambled and engaged in other more complicated and contract-based interoperations via Bytom.\r\n\r\nWhile Ethereum’s SEC scrutinization as a possible security took the entire crypto market on a downturn in early 2018, Bytom voluntarily submitted to the SEC’s Howey Test and Bytom cryptocurrency was deemed not a security under its DAO watch. This one has potential to make an impact on the cryptocurrency market with low transaction fees, high tech Bytom blockchain technology and an interactive protocol of multiple byte assets that could mark it apart. A Bytom wallet is also on offer and can store plenty more than Bytom coins.'},
                            'links': {'homepage': ['http://bytom.io/', '', ''],
                                      'blockchain_site': ['https://etherscan.io/token/0xcb97e65f07da24d46bcdd078ebebd7c6e6e3d750', 'http://btmscan.com/', 'https://btm.tokenview.com/',
                                                          'https://blockmeta.com/', '', '', '', '', '', ''], 'official_forum_url': ['', '', ''], 'chat_url': ['', '', ''], 'announcement_url': ['', ''],
                                      'twitter_screen_name': 'Bytom_Official', 'facebook_username': '', 'bitcointalk_thread_identifier': 1975390, 'telegram_channel_identifier': '',
                                      'subreddit_url': 'https://www.reddit.com/r/BytomBlockchain', 'repos_url': {'github': ['https://github.com/bytom/bytom'], 'bitbucket': []}},
                            'image': {'thumb': 'https://assets.coingecko.com/coins/images/1087/thumb/2qNyrhUxEmnGCKi.png?1630048151',
                                      'small': 'https://assets.coingecko.com/coins/images/1087/small/2qNyrhUxEmnGCKi.png?1630048151',
                                      'large': 'https://assets.coingecko.com/coins/images/1087/large/2qNyrhUxEmnGCKi.png?1630048151'}, 'country_origin': 'CN', 'genesis_date': '2017-10-31',
                            'contract_address': '0xcb97e65f07da24d46bcdd078ebebd7c6e6e3d750', 'sentiment_votes_up_percentage': 100.0, 'sentiment_votes_down_percentage': 0.0,
                            'ico_data': {'ico_start_date': '2017-06-20T00:00:00.000Z', 'ico_end_date': '2017-07-20T00:00:00.000Z', 'short_desc': 'Transfer assets from atomic world to byteworld',
                                         'description': None, 'links': {}, 'softcap_currency': '', 'hardcap_currency': '', 'total_raised_currency': 'USD', 'softcap_amount': None,
                                         'hardcap_amount': None,
                                         'total_raised': '2286000.0', 'quote_pre_sale_currency': '', 'base_pre_sale_amount': None, 'quote_pre_sale_amount': None, 'quote_public_sale_currency': 'USD',
                                         'base_public_sale_amount': 1.0, 'quote_public_sale_amount': 0.05, 'accepting_currencies': '', 'country_origin': 'CN', 'pre_sale_start_date': None,
                                         'pre_sale_end_date': None, 'whitelist_url': '', 'whitelist_start_date': None, 'whitelist_end_date': None, 'bounty_detail_url': '', 'amount_for_sale': None,
                                         'kyc_required': True, 'whitelist_available': None, 'pre_sale_available': None, 'pre_sale_ended': False}, 'market_cap_rank': 596, 'coingecko_rank': 127,
                            'coingecko_score': 43.875, 'developer_score': 69.542, 'community_score': 32.113, 'liquidity_score': 32.393, 'public_interest_score': 0.002,
                            'public_interest_stats': {'alexa_rank': 863004, 'bing_matches': None}, 'status_updates': [], 'last_updated': '2022-02-01T08:47:38.964Z'},
          'icon_url': 'https://assets.coingecko.com/coins/images/1087/large/2qNyrhUxEmnGCKi.png?1630048151', 'coin_id': 'cv-btm-bytom', 'lazy_load': True, 'created_at': 1643705616,
          'GS1Hash': 'CRYPTO_COINS', 'GS1Sort': 'cv-btm-bytom'},
         ]

    for i in a:
        print(ddb.type_serialize(i))
