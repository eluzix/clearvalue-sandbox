import concurrent.futures
import csv
import json
import time

import boto3
import numpy as np

import cvutils
from clearvalue import app_config
from cvanalytics import iter_active_users
from cvcore.calcs.portfolio import portfolio_holding_stats
from cvcore.model.cv_types import AccountTypes, AccountStatus
from cvcore.store import loaders, DBKeys
from cvutils import TerminalColors


def collect_asset_allocation():
    tp1 = time.time()
    all_allocations = []
    allocation_segments = {}
    total_users = 0
    for user in iter_active_users(load_latest=True, active_only=False):
        asset_allocation = user.get('asset_allocation')
        if asset_allocation is not None:
            total_users += 1
            all_allocations.append(asset_allocation)
            aum_segment = user.get('aum_segment', 0)
            if aum_segment > 0:
                aum_segment = f's:{aum_segment}'
                segment_data = allocation_segments.get(aum_segment, [])
                segment_data.append(asset_allocation)
                allocation_segments[aum_segment] = segment_data
    with open('crowd_asset_allocation.json', 'w') as fout:
        res = {'all': all_allocations, 'total_users': total_users}
        res.update(allocation_segments)
        json.dump(res, fout, cls=cvutils.ValueEncoder)

    tp2 = time.time()
    print(f'{TerminalColors.OK_GREEN}All done in {TerminalColors.WARNING}{tp2 - tp1}{TerminalColors.END}')


def _all_asset_types():
    return [i.value for i in AccountTypes if i not in
            [AccountTypes.ANGEL_INVESTMENT, AccountTypes.HEDGE_FUND]]


def analyze_asset_allocation(seg_key='all'):
    all_types = _all_asset_types()
    all_stats = {t: [] for t in all_types}
    with open('crowd_asset_allocation.json', 'r') as fin:
        js = json.load(fin)

    total_users = js['total_users']

    for item in js[seg_key]:
        for at in all_stats:
            all_stats[at].append(item.get(at, 0))

    total_mean = 0
    res = {}
    for at in all_stats:
        at_stats = all_stats[at]
        at_length = len(all_stats[at])
        at_value_stats = [v for v in all_stats[at] if v > 0]
        mean = np.mean(at_stats)
        value_mean = np.mean(at_value_stats)
        p95 = np.percentile(at_stats, 50)
        var = np.var(at_stats)
        total_mean += mean
        print(f'For {TerminalColors.FAIL}{at}{TerminalColors.END} '
              f'avg. holding is {TerminalColors.OK_CYAN}{mean:.2%}{TerminalColors.END}'
              f' and p95 is {TerminalColors.OK_GREEN}{p95}{TerminalColors.END}'
              f' and VAR is {TerminalColors.OK_GREEN}{var:0.2%}{TerminalColors.END},'
              f'\n\t\ttotal values: {len(at_value_stats)} and mean is {value_mean:0.2%}'
              )
        res[at] = {'segment': seg_key, 'mean': mean, 'p95': p95, 'var': var,
                   'value_mena': value_mean, 'total_with_values': len(at_value_stats),
                   'total_users': total_users, 'segment_total': at_length}
    return res


def dump_asset_allocation():
    # collect_asset_allocation()
    all_types = _all_asset_types()
    with open('asset_allocation.csv', 'w') as fout:
        writer = csv.writer(fout)
        headers = [t for t in all_types]
        headers.insert(0, 'Segment')
        # writer.writerow(['Segment', 'Asset Type', 'Mean', 'P95', 'Var'])
        writer.writerow(headers)
        for k in ['all', 's:1', 's:2', 's:3', 's:4']:
            print(f'{TerminalColors.HEADER}Running for {k}{TerminalColors.END}')
            res = analyze_asset_allocation(k)
            row = [res.get(at, {'mean': 0})['mean'] for at in all_types]
            row.insert(0, k)
            writer.writerow(row)

    print(f'{TerminalColors.WARNING}dump_asset_allocation done{TerminalColors.END}')


def dump_holding_percent():
    all_types = _all_asset_types()
    with open('asset_holding_percent.csv', 'w') as fout:
        writer = csv.writer(fout)
        headers = [t for t in all_types]
        headers.insert(0, 'Segment')
        # writer.writerow(['Segment', 'Asset Type', 'Mean', 'P95', 'Var'])
        writer.writerow(headers)
        for k in ['all', 's:1', 's:2', 's:3', 's:4']:
            print(f'{TerminalColors.HEADER}Running for {k}{TerminalColors.END}')
            res = analyze_asset_allocation(k)
            row = [res[at]['total_with_values'] / res[at]['segment_total'] for at in all_types]
            row.insert(0, k)
            writer.writerow(row)

    print(f'{TerminalColors.WARNING}dump_holding_percent done{TerminalColors.END}')


def _dump_user_holdings(uid):
    active_status = [AccountStatus.ACTIVE.value, AccountStatus.CLOSED.value]
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO,
                                          load_status=active_status)
    accounts = [a for a in accounts if a.get('is_high_level') is not True]
    all_holdings = []
    for account in accounts:
        holdings = loaders.load_account_holdings(account['account_id'])
        all_holdings.extend(holdings)

    stats = {}
    if len(all_holdings) > 0:
        stats = portfolio_holding_stats(all_holdings)

    return stats


def dump_all_users_holdings():
    all_stats = []
    future_to_user = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for user in iter_active_users(load_latest=True, active_only=False):
            aum_segment = user.get('aum_segment', 0)
            if aum_segment == 0:
                continue
            future_id = executor.submit(_dump_user_holdings, user[DBKeys.HASH_KEY])
            future_to_user[future_id] = user

        for future in concurrent.futures.as_completed(future_to_user):
            user = future_to_user[future]
            stats = future.result()
            stats['aum_segment'] = user.get('aum_segment', 0)
            all_stats.append(stats)

    with open('users_holdings.json', 'w') as fout:
        json.dump(all_stats, fout)

    print(f'{TerminalColors.WARNING}dump_all_users_holdings done{TerminalColors.END}')


def _all_security_types():
    return ['etf', 'equity', 'mutual fund', 'bond', 'option', 'mutualfund', 'other']


def _all_sectors():
    return ['realestate', 'consumer cyclical',
            'basic materials', 'consumer defensive',
            'technology', 'communication services',
            'financial services', 'utilities',
            'industrials', 'energy',
            'healthcare', 'other',
            'real estate', 'services']


def _color(val, color):
    return f'{color}{val}{TerminalColors.END}'


def _colorp(val, color):
    return f'{color}{val:0.2%}{TerminalColors.END}'


def analyze_all_users_holdings():
    with open('users_holdings.json', 'r') as fin:
        js = json.load(fin)

    all_holding_types = {}
    all_sectors = {}
    for user in js:
        holding_types = user.get('holding_types', {})
        for ht in _all_security_types():
            ht_data = all_holding_types.get(ht, [])
            val = float(holding_types.get(ht, {}).get('total_value', 0))
            if val > 100000000:
                continue
            ht_data.append(val)
            all_holding_types[ht] = ht_data

        sectors = user.get('sectors', {})
        for sector in _all_sectors():
            sector_data = all_sectors.get(sector, [])
            val = float(sectors.get(sector, {}).get('total_value', 0))
            if val > 100000000:
                continue
            sector_data.append(val)
            # sector_data.append(abs(sectors[sector]['percent']))
            all_sectors[sector] = sector_data

    print(f'{TerminalColors.HEADER}------ Types ------{TerminalColors.END}')
    _total_holding_types = 0
    for ht in all_holding_types:
        _total_holding_types += np.sum(all_holding_types[ht])

    for ht in all_holding_types:
        mean = np.sum(all_holding_types[ht])
        print(f'For {_color(ht, TerminalColors.OK_CYAN)} '
              f'total: {_color(_total_holding_types, TerminalColors.OK_GREEN)} '
              f'mean: {_color(mean, TerminalColors.OK_GREEN)} '
              f'result: {_color(mean / _total_holding_types, TerminalColors.OK_GREEN)} '
              f'')
    # print(f'>>>>>>> total: {_total}')

    print(f'{TerminalColors.HEADER}------ Sectors ------{TerminalColors.END}')
    _total_sectors = 0
    for sector in all_sectors:
        _total_sectors += np.sum(all_sectors[sector])

    for sector in all_sectors:
        mean = np.sum(all_sectors[sector])
        # print(f'For {_color(sector, TerminalColors.OK_CYAN)} '
        #       f'total: {_color(_total_sectors, TerminalColors.OK_GREEN)} '
        #       f'mean: {_color(mean, TerminalColors.OK_GREEN)} '
        #       f'result: {_color(mean / _total_sectors, TerminalColors.OK_GREEN)} '
        #       f'')
        print(f'{sector}, {mean}, {mean / _total_sectors}')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # dump_asset_allocation()
    # dump_holding_percent()

    # _dump_user_holdings('2bb40134-1a88-4491-bedf-496401a429f0')
    # dump_all_users_holdings()
    analyze_all_users_holdings()
