import csv
import json

import boto3

from clearvalue import app_config
from cvanalytics import query_cursor
from clearvalue.lib.search import elastic


def dump_json():
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    client = elastic.client(boto_session=boto_session)

    data = {}
    query = {
        'bool': {
            "should": [
                {"exists": {"field": "total_joins"}},
                {"exists": {"field": "active_stats_segment_count"}},
                {"exists": {"field": "active_45days_30age_stats_segment_count"}},
                {"exists": {"field": "active_1type_30age_4sessions_stats_segment_count"}},
            ],
            'filter': [{
                "range": {
                    "report_date": {
                        "gte": '2020-11-01',
                        "lte": '2021-10-13',
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }

    for hit in query_cursor(client, query, index='kpi*'):
        source = hit['_source']

        date_data = data.get(source['report_date'], {})
        if 'active_stats_segment_count' in source:
            date_data['active_stats_segment_count'] = source['active_stats_segment_count']
        if 'active_45days_30age_stats_segment_count' in source:
            date_data['active_45days_30age_stats_segment_count'] = source['active_45days_30age_stats_segment_count']
        if 'active_1type_30age_4sessions_stats_segment_count' in source:
            date_data['active_1type_30age_4sessions_stats_segment_count'] = source['active_1type_30age_4sessions_stats_segment_count']
        data[source['report_date']] = date_data

    for hit in query_cursor(client, query, index='reports*'):
        source = hit['_source']

        date_data = data.get(source['report_date'], {})
        if 'total_joins' in source:
            date_data['total_joins'] = source['total_joins']
        data[source['report_date']] = date_data

    with open('active_vs_new.json', 'w') as fout:
        json.dump(data, fout)


def dump_csv():
    with open('active_vs_new.json', 'r') as fin:
        js = json.load(fin)

    with open('active_vs_new.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(['Date', 'Active 30', 'Active 45', 'Intercom', 'Joins'])
        keys = list(js.keys())
        keys.sort()
        for report_date in keys:
            source = js[report_date]
            writer.writerow([report_date, source['active_stats_segment_count'],
                             source['active_45days_30age_stats_segment_count'], source['active_1type_30age_4sessions_stats_segment_count'],
                             source['total_joins']])


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    dump_json()
    dump_csv()
    print('all done')
