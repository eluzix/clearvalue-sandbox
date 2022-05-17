import csv
import datetime
import json
import time

import boto3

from clearvalue import app_config
from cvanalytics import query_cursor, is_internal_user
from clearvalue.lib import utils
from clearvalue.lib.search import elastic


def load_daily_data(client, current_date: datetime.datetime):
    date_str = utils.date_to_str(current_date)
    ret = {}

    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "gte": date_str,
                        "lte": date_str,
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        },
    }

    for hit in query_cursor(client, query, query_extra={'_source': ['uid', 'ts']}):
        source = hit['_source']
        uid = source.get('uid')
        if uid is None:
            continue

        if is_internal_user(uid):
            continue

        user_data = ret.get(uid, [])
        user_data.append(source['ts'])
        ret[uid] = user_data

    return ret


def reduce_day_data(data):
    total_day_data = 0

    for uid in data:
        sessions = data[uid]
        if len(sessions) < 2:
            continue

        sessions.sort()
        start_date = datetime.datetime.strptime(sessions[0], '%Y-%m-%dT%H:%M:%S.%f%z')
        end_date = datetime.datetime.strptime(sessions[-1], '%Y-%m-%dT%H:%M:%S.%f%z')
        sessions_time = (end_date - start_date).seconds
        total_day_data += sessions_time

    return total_day_data


def generate_activity_data(boto_session):
    tp1 = time.time()
    client = elastic.client(boto_session=boto_session)
    start_date = utils.date_from_str('2020-11-01')
    run_date = start_date
    today = utils.today()
    data = {}

    while run_date < today:
        run_date_str = utils.date_to_str(run_date)
        print(f'Running for {run_date_str}')
        day_data = load_daily_data(client, run_date)
        total_day_data = reduce_day_data(day_data)
        data[run_date_str] = total_day_data
        run_date = run_date + datetime.timedelta(days=1)

    with open('usage_per_day.json', 'w') as fout:
        json.dump(data, fout)

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def parse_usage():
    tp1 = time.time()

    with open('usage_per_day.json', 'r') as fin:
        js = json.load(fin)

    with open('usage_per_day.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['date', 'total seconds', 'total minutes'])
        for dt in js:
            writer.writerow([dt, str(js[dt]), str(round(js[dt] / 60, 2))])

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


if __name__ == '__main__':
    profile = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # generate_activity_data(profile)
    parse_usage()
