import csv
import json
import time

import boto3

from clearvalue import app_config
from clearvalue.analytics import iter_active_users, is_user_active
from clearvalue.lib import TerminalColors


def report():
    tp1 = time.time()
    all_users = {}
    active_users = {}
    group3 = {}

    for user in iter_active_users(load_latest=True, active_only=False):
        utm_campaign = user.get('utm_campaign', 'no-campaign').lower()
        total_sessions_time = float(user.get('total_sessions_time', 0.0))

        campaign_stat = all_users.get(utm_campaign, [0, 0])
        campaign_stat[0] += 1
        campaign_stat[1] += total_sessions_time
        all_users[utm_campaign] = campaign_stat

        if is_user_active(user):
            campaign_stat = active_users.get(utm_campaign, [0, 0])
            campaign_stat[0] += 1
            campaign_stat[1] += total_sessions_time
            active_users[utm_campaign] = campaign_stat

        total_sessions = user.get('total_sessions', 0)
        if total_sessions >= 10:
            campaign_stat = group3.get(utm_campaign, [0, 0])
            campaign_stat[0] += 1
            campaign_stat[1] += total_sessions_time
            group3[utm_campaign] = campaign_stat

    with open('campaign_report.json', 'w') as fout:
        json.dump({'all': all_users, 'active': active_users, 'sessions': group3}, fout)

    tp2 = time.time()
    print(f'*** {TerminalColors.OK_GREEN}All done in {tp2 - tp1}{TerminalColors.END} ***')


def dump_csv(key, file_name):
    with open('campaign_report.json', 'r') as fin:
        js = json.load(fin)

    data = js[key]
    data = [(k, int(data[k][0]), round(float(data[k][1]), 2), round(float(data[k][1]) / int(data[k][0]), 2)) for k in data]
    data.sort(key=lambda i: i[1], reverse=True)
    with open(file_name, 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['Campaign', 'Total Users', 'Total Session Time', 'Avg. Session Time'])
        writer.writerows(data)

    print(f'*** {TerminalColors.OK_GREEN}All done for {key}{TerminalColors.END} ***')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # report()
    dump_csv('all', 'campaign_report_all.csv')
    dump_csv('active', 'campaign_report_active.csv')
    dump_csv('sessions', 'campaign_report_sessions.csv')
