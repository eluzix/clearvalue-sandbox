import copy
import json
import statistics
import time

import boto3
import concurrent.futures

import numpy as np

import cvutils
from clearvalue import app_config
from cvcore.calcs.portfolio import portfolio_holding_stats
from cvcore.model.cv_types import HoldingStatus
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors
from cvutils.dynamodb import ddb


def dump_transactions(start_date, end_date):
    tp1 = time.time()
    ed = cvutils.timestamp_from_string(end_date)

    js = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_user = {
            executor.submit(loaders.load_user_transactions, user[DBKeys.HASH_KEY],
                            since_date=start_date, to_date=end_date): user
            for user in loaders.iter_users() if user['created_at'] <= ed
        }

        for future in concurrent.futures.as_completed(future_to_user):
            user = future_to_user[future]
            transactions = future.result()
            js[user[DBKeys.HASH_KEY]] = transactions
    with open(f'transactions_{start_date}_{end_date}.json', 'w') as fout:
        json.dump(js, fout)

    tp2 = time.time()
    print(f'done in {TerminalColors.OK_GREEN}{tp2 - tp1}{TerminalColors.END}')


def generate_insight(file_name):
    with open(file_name, 'r') as fin:
        js = json.load(fin)

    all_data = {}
    all_users = []
    for uid in js:
        transactions = js[uid]
        if len(transactions) == 0:
            continue

        all_users.append(len(transactions))
        for tr in transactions:
            transaction_type = tr['transaction_type']
            type_data = all_data.get(transaction_type, {'values': []})
            type_data['values'].append(tr.get('value', 0))
            all_data[transaction_type] = type_data

    for tr_type in all_data:
        type_data = all_data[tr_type]
        count = len(type_data['values'])
        _sum = round(np.sum(type_data['values']))
        mean = round(np.mean(type_data['values']))
        # print(f'For {TerminalColors.WARNING}{tr_type}{TerminalColors.END}, count: {count}, mean: {mean}')
        print(f'{tr_type},{mean},{_sum}')

    # print(all_users)
    avg_tr = np.mean(all_users)
    print(f'Avg transactions per user {TerminalColors.WARNING}{avg_tr}{TerminalColors.END}')


def process_allocations():
    sort_key = DBKeys.account_time_point('2022-01-10')
    table_name = app_config.resource_name('accounts')
    current_allocation = {}
    previous_allocation = {}
    file_name = '/Users/uzix/Downloads/january-accounts.json'
    with open(file_name, 'r') as fin:
        js = json.load(fin)
        for uid in js:
            keys = []
            current_accounts = {}

            for account in js[uid]:
                account_id = account['account_id']
                account_type = account['account_type']
                account_value = account.get('value', 0)

                type_data = current_allocation.get(account_type, [])
                type_data.append(account_value)
                current_allocation[account_type] = type_data

                keys.append(DBKeys.hash_sort(account_id, sort_key))
                current_accounts[account_id] = account

            tp_accounts = ddb.batch_get_items(table_name, keys)
            for tp_ac in tp_accounts:
                account_id = tp_ac[DBKeys.HASH_KEY].replace(DBKeys.account_time_point(''), '')
                ac = current_accounts[account_id]
                account_type = ac['account_type']
                account_value = tp_ac.get('value', 0)

                type_data = previous_allocation.get(account_type, [])
                type_data.append(account_value)
                previous_allocation[account_type] = type_data

    file_name = f'{file_name}_alloc.json'
    with open(file_name, 'w') as fout:
        json.dump({'current': current_allocation, 'previous': previous_allocation}, fout, cls=cvutils.ValueEncoder)


def _clean_up_values(ar):
    return [v for v in ar if v < 100000000]


def process_allocations_insights():
    file_name = '/Users/uzix/Downloads/january-accounts.json_alloc.json'
    data = {}
    with open(file_name, 'r') as fin:
        js = json.load(fin)
        total = 0
        for at in js['current']:
            at_data = _clean_up_values(js['current'][at])
            total += np.sum(at_data)
        for at in js['current']:
            at_data = _clean_up_values(js['current'][at])
            data[at] = {'current': np.sum(at_data) / total}
            # print(f'For {at} total: {total}, val: {np.sum(at_data)}, %: {(np.sum(at_data)/total)*100:.2f}')

        total = 0
        for at in js['previous']:
            at_data = _clean_up_values(js['previous'][at])
            total += np.sum(at_data)
        for at in js['previous']:
            at_data = _clean_up_values(js['previous'][at])
            td = data.get(at, {})
            td['previous'] = np.sum(at_data) / total
            data[at] = td
            # print(f'For {at} total: {total}, val: {np.sum(at_data)}, %: {(np.sum(at_data)/total)*100:.2f}')

    for at in data:
        td = data[at]
        print(f"{at}, {td.get('previous', 0)}, {td.get('current', 0)}")


def process_sp_allocation(load_history_symbols=False):
    # sort_key = DBKeys.account_time_point('2022-01-10')
    tp = cvutils.timestamp_from_string('2022-01-10')
    tp_date = cvutils.date_from_timestamp(tp)
    # table_name = app_config.resource_name('accounts')
    current_allocation = {}
    previous_allocation = {}
    file_name = '/Users/uzix/Downloads/january-accounts.json'
    # all_sp_accounts = {}
    # with open(file_name, 'r') as fin:
    #     js = json.load(fin)
    #     for uid in js:
    #         sp_accounts = []
    #
    #         for account in js[uid]:
    #             account_id = account['account_id']
    #             account_type = account['account_type']
    #             if account_type != 'sp':
    #                 continue
    #             if account.get('is_high_level') is True:
    #                 continue
    #
    #             sp_accounts.append(account)
    #         all_sp_accounts[uid] = sp_accounts
    #
    # with open(f'{file_name}-sp.json', 'w') as fout:
    #     json.dump(all_sp_accounts, fout)

    user_data = {}
    kwargs = {
        'load_active_only': False,
        'ProjectionExpression':
            'symbol,created_at,holding_status,deleted_at,holding_type,quantity,#value,purchase_price',
        'ExpressionAttributeNames': {'#value': 'value'}

    }
    with open('/Users/uzix/Downloads/january-holdings.json', 'w') as fout:
        with open(f'{file_name}-sp.json', 'r') as fin:
            js = json.load(fin)
            total_users = len(js)
            count = 0
            for uid in js:
                count += 1
                print(f'processing {count} out of {total_users} ({round((count/total_users)*100, 2)}%)')

                user_current_holdings = []
                user_previous_holdings = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_holdings = {
                        executor.submit(loaders.load_account_holdings,
                                        account['account_id'], **kwargs): account
                        for account in js[uid]
                    }

                    for future in concurrent.futures.as_completed(future_to_holdings):
                        account = future_to_holdings[future]
                        holdings = future.result()
                        for h in holdings:
                            if h['value'] > 100000000:
                                continue

                            if h.get('holding_status', HoldingStatus.ACTIVE.value) == HoldingStatus.ACTIVE.value:
                                user_current_holdings.append(copy.copy(h))
                            if h['created_at'] <= tp:
                                user_previous_holdings.append(copy.copy(h))
                if load_history_symbols:
                    previous_symbols = list(set([h.get('symbol') for h in user_previous_holdings if h.get('symbol') is not None]))
                    if len(previous_symbols) > 0:
                        previous_symbols_data = loaders.load_securities(previous_symbols, for_date=tp_date, skip_past_search=True)
                        if len(previous_symbols_data) > 0:
                            for ph in user_previous_holdings:
                                symbol = ph.get('symbol')
                                quantity = ph.get('quantity')
                                if symbol is not None and quantity is not None and quantity > 0:
                                    pi = previous_symbols_data.get(symbol)
                                    if pi is not None:
                                        price = cvutils.get_symbol_price(pi, default=0)
                                        ph['value'] = quantity * price
                json.dump({'uid': uid, 'current': user_current_holdings, 'previous': user_previous_holdings}, fout, cls=cvutils.ValueEncoder)
                fout.write('\n')
                # user_data[uid] = {'current': user_current_holdings, 'previous': user_previous_holdings}

    # with open('/Users/uzix/Downloads/january-holdings.json', 'w') as fout:
    #     json.dump(user_data, fout, cls=cvutils.ValueEncoder)

    print('all done')


def process_sp_holdings():
    with open('/Users/uzix/Downloads/january-holdings-enriched.json', 'w') as fout:
        with open('/Users/uzix/Downloads/january-holdings.json', 'r') as fin:
            total_lines = 1414
            count = 0
            for line in fin:
                count += 1
                print(f'processing {count} out of {total_lines} ({round((count/total_lines)*100, 2)}%)')

                js = json.loads(line)
                # uid = js['uid']
                if len(js['current']) > 0:
                    current_stats = portfolio_holding_stats(js['current'])
                else:
                    current_stats = {}

                if len(js['previous']) > 0:
                    previous_stats = portfolio_holding_stats(js['previous'])
                else:
                    previous_stats = {}

                js['current-stats'] = current_stats
                js['previous-stats'] = previous_stats

                json.dump(js, fout, cls=cvutils.ValueEncoder)
                fout.write('\n')

    print('[process_sp_holdings] all done')


def generate_holdings_insights():
    previous_types_values = {}
    current_types_values = {}
    # previous_types_counts = {}
    # current_types_counts = {}
    with open('/Users/uzix/Downloads/january-holdings-enriched.json', 'r') as fin:
        total_lines = 1414
        count = 0
        # previous_count = 0
        # current_count = 0
        for line in fin:
            # print(f'processing {count} out of {total_lines} ({round((count/total_lines)*100, 2)}%)')

            js = json.loads(line)
            if len(js['current']) == 0 and len(js['previous']) == 0:
                continue

            count += 1
            current_types = js['current-stats'].get('holding_types', {})
            for ht in current_types:
                # current_count += 1
                td = current_types_values.get(ht, [])
                p = current_types[ht]['percent']
                td.append(round(p, 4))
                current_types_values[ht] = td

            previous_types = js['previous-stats'].get('holding_types', {})
            for ht in previous_types:
                # previous_count += 1
                td = previous_types_values.get(ht, [])
                p = previous_types[ht]['percent']
                td.append(round(p, 4))
                previous_types_values[ht] = td

    final = {}
    print(f'--------\nPREVIOUS\n--------')
    longest_line = max([len(previous_types_values[at]) for at in previous_types_values])
    for at in previous_types_values:
        # vals = [v for v in previous_types_values[at]]
        vals = previous_types_values[at]
        # vals = np.pad(vals, (0, longest_line-len(vals)), constant_values=0)
        vals = np.pad(vals, (0, count-len(vals)), constant_values=0)

        final[at] = {'p': round(np.mean(vals), 4), 'c': 0}
        print(f'{at},{np.mean(vals)}, {len(vals)}, {len(previous_types_values[at])}')

    print(f'-------\nCURRENT\n-------')
    longest_line = max([len(current_types_values[at]) for at in current_types_values])
    for at in current_types_values:
        vals = current_types_values[at]
        # vals = np.pad(vals, (0, longest_line-len(vals)), constant_values=0)
        vals = np.pad(vals, (0, count-len(vals)), constant_values=0)

        final_data = final.get(at, {'p': 0})
        final_data['c'] = round(np.mean(vals), 4)
        final[at] = final_data
        print(f'{at},{np.mean(vals)}, {len(vals)}, {len(current_types_values[at])}')

    print('------------------------')
    print('Final Results')
    print('------------------------')
    for at in final:
        print(f'{at}, {final[at]["p"]}, {final[at]["c"]}')
    print('[generate_holdings_insights] all done')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # process_allocations()
    # process_allocations_insights()

    process_sp_allocation()
    process_sp_holdings()
    generate_holdings_insights()


    # dump_transactions('2021-05-01', '2021-06-21')
    # files = ['transactions_2022-01-01_2022-02-21', 'transactions_2022-05-01_2022-06-21']
    # for fn in files:
    #     print(f'Processing {TerminalColors.WARNING}{fn}{TerminalColors.END}')
    #     generate_insight(f'/Users/uzix/Downloads/{fn}.json')
