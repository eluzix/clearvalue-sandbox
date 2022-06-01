import csv
import datetime

import boto3
import numpy as np

import cvutils
from clearvalue import app_config
from clearvalue.analytics import query_cursor, ignore_user_analytics
from cvcore.store import DBKeys
from cvutils import elastic
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    client = elastic.client(boto_session=boto_session)

    start_date = '2022-01-01'
    break_date = cvutils.date_from_str('2022-02-08')

    query = {
        'bool': {
            "must": [
                {"match": {"log_name": "client-log"}},
                {"match": {"category": "signup"}}
            ],
            'filter': [{
                "range": {
                    "ts": {
                        "gte": start_date,
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }

    groups = {1: set(), 2: set()}
    for hit in query_cursor(client, query):
        source = hit['_source']
        uid = source.get('uid')
        if uid is None:
            continue
        if ignore_user_analytics(uid):
            continue

        doc_date = datetime.datetime.strptime(source['ts'], '%Y-%m-%dT%H:%M:%S.%f%z')
        group = 1 if doc_date < break_date else 2
        groups[group].add(uid)

    all_ids = set()
    all_ids.update(groups[1])
    all_ids.update(groups[2])

    table_name = app_config.resource_name('analytics')
    stats_records = ddb.batch_get_items(table_name, [DBKeys.hash_sort(uid, 'STATS') for uid in all_ids])
    stats_records = {r[DBKeys.HASH_KEY]: r for r in stats_records}

    meta_records = ddb.batch_get_items(table_name, [DBKeys.hash_sort(uid, 'META_STATS') for uid in all_ids])
    meta_records = {r[DBKeys.HASH_KEY]: r for r in meta_records}

    data = {1: {}, 2: {}}
    avg_fields = ['total_sessions', 'total_liability_types',
                  'last_session_age', 'avg_session_duration', 'user_age',
                  'total_account_types', 'total_asset_types', 'total_sessions_time',
                  'avg_time_between_sessions', 'total_active_accounts',
                  'linked_accounts', 'avg_accounts_age', 'total_transactions']
    for group in groups:
        group_data = data[group]
        for uid in groups[group]:
            stat_record = stats_records.get(uid)
            meta_record = meta_records.get(uid)
            if stat_record is None:
                continue
            count = group_data.get('count', 0)
            group_data['count'] = count + 1
            group_data['group'] = group

            aum_group = stat_record.get('aum_segment', 0)
            aum_key = f'aum_group.{aum_group}'
            group_data[aum_key] = group_data.get(aum_key, 0) + 1

            create_count = 0 if meta_record is None else meta_record.get('first_session_create_count', 0)
            if create_count == 0:
                create_key = f'first_create.no assets'
            elif create_count < 4:
                create_key = f'first_create.1-3 assets'
            else:
                create_key = f'first_create.4+ assets'
            group_data[create_key] = group_data.get(create_key, 0) + 1

            for avg_field in avg_fields:
                field_data = group_data.get(avg_field, [])
                field_data.append(stat_record.get(avg_field, 0))
                group_data[avg_field] = field_data

    for group in data:
        group_data = data[group]
        for avg_field in avg_fields:
            if avg_field in group_data:
                field_data = [0 if a == '-' else a for a in group_data.get(avg_field)]
                field_mean = float(np.mean(field_data))
                field_median = float(np.median(field_data))
                del group_data[avg_field]
            else:
                field_mean = 0
                field_median = 0
                field_std = 0
                field_percentile = 0
                field_min = 0
                field_max = 0
            group_data[f'{avg_field}_mean'] = field_mean
            group_data[f'{avg_field}_median'] = field_median

    headers = data[1].keys()
    with open('users_comparison.csv', 'w') as fout:
        writer = csv.writer(fout)
        hr = ['Group']
        hr.extend(headers)
        writer.writerow(hr)
        for group in data:
            row = [str(group)]
            row.extend([str(data[group][h]) for h in headers])
            writer.writerow(row)

    print('*** all done ***')
