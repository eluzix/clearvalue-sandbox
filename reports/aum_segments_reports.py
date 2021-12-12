import csv
import json
from collections import Counter

import boto3

from clearvalue import app_config


def _aum_label(segment):
    if segment == 1:
        return '0-$500K'
    elif segment == 2:
        return '$500K-$1M'
    elif segment == 3:
        return '$1M-$5M'
    elif segment == 0:
        return 'No Accounts'

    return "$5M+"


def user_campaign_map():
    with open('uc_map.json', 'r') as fin:
        uc_map = json.load(fin)

    return uc_map
    # ret = {}
    # for user in iter_active_users(load_latest=True, active_only=False):
    #     uid = user[DBKeys.HASH_KEY]
    #     utm_campaign = user.get('utm_campaign', 'no-campaign').lower()
    #     ret[uid] = (user, utm_campaign)
    #
    # with open('uc_map.json', 'w') as fout:
    #     json.dump(ret, fout)
    # return ret


def campaign_report():
    users = user_campaign_map()

    campaigns = {}
    for uid in users:
        user, campaign = users[uid]
        campaign_data = campaigns.get(campaign, [])
        aum_segment = user['aum_segment']
        if user.get('total_account_types', 0) == 0:
            aum_segment = 0

        campaign_data.append(aum_segment)
        campaigns[campaign] = campaign_data

    for campaign in campaigns:
        campaign_data = campaigns[campaign]
        if len(campaign_data) < 10:
            continue

        total_values = len(campaign_data)
        counter = Counter(campaign_data)
        # print(f'AUM for campaign "{campaign}"')
        segments = [s for s in counter]
        segments.sort()
        row = [f'%{(counter[segment] / total_values) * 100:.2f}' for segment in segments]
        print(f'AUM for campaign "{campaign}" ({total_values}): {row}')
        # for segment in segments:
        #     print(f'Segment {_aum_label(segment)} is %{(counter[segment] / total_values) * 100:.2f} of campaign')


def terms_report():
    users = user_campaign_map()

    terms = {}
    for uid in users:
        user, _ = users[uid]
        term = user.get('utm_term', 'NO TERM')
        term_data = terms.get(term, [])
        aum_segment = user['aum_segment']
        if user.get('total_account_types', 0) == 0:
            aum_segment = 0

        term_data.append(aum_segment)
        terms[term] = term_data

    with open('aum_terms.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['Term', 'Total', '$0 Networth', '$0-$500K', '$500K-$1M', '$1M-$5M', '$5M+'])
        for term in terms:
            term_data = terms[term]
            if len(term_data) < 10:
                continue

            total_values = len(term_data)
            counter = Counter(term_data)
            # print(f'AUM for term "{term}"')
            all_terms = [s for s in counter]
            all_terms.sort()
            values = ['0', '0', '0', '0', '0']
            for i, t in enumerate(all_terms):
                # values[i] = f'%{(counter[t] / total_values) * 100:.2f}'
                values[i] = str(counter[t] / total_values)
            row = [term, str(total_values)]
            row.extend(values)
            writer.writerow(row)
            print(row)
            # row = [f'{_aum_label(t)} %{(counter[t] / total_values) * 100:.2f}' for t in all_terms]
            # values = ','.join(values)
            # print(f'{term}, {total_values}, {values}')
            # for segment in segments:
            #     print(f'Segment {_aum_label(segment)} is %{(counter[segment] / total_values) * 100:.2f} of term')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # campaign_report()
    terms_report()
