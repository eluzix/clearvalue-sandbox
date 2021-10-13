import boto3
from elasticsearch.helpers import bulk

from clearvalue import app_config
from clearvalue.analytics import query_cursor
from clearvalue.lib.search import elastic


def _get_active_info(client, start_date, end_date):
    query = {
        'bool': {
            "should": [
                {"exists": {"field": "active_45days_30age_stats_segment_count"}},
                {"exists": {"field": "active_60days_30age_stats_segment_count"}},
                {"exists": {"field": "active_90days_30age_stats_segment_count"}},
            ],
            'filter': [{
                "range": {
                    "report_date": {
                        "gte": start_date,
                        "lte": end_date,
                        'format': 'yyyy-MM-dd'
                    }
                }
            }],
        }
    }

    ret = {}
    for hit in query_cursor(client, query, index='kpis*'):
        source = hit['_source']
        date_data = ret.get(source['report_date'], {})
        if 'active_45days_30age_stats_segment_count' in source:
            date_data['45'] = source['active_45days_30age_stats_segment_count']
        if 'active_60days_30age_stats_segment_count' in source:
            date_data['60'] = source['active_60days_30age_stats_segment_count']
        if 'active_90days_30age_stats_segment_count' in source:
            date_data['90'] = source['active_90days_30age_stats_segment_count']
        ret[source['report_date']] = date_data

    return ret


if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    client = elastic.client(boto_session=boto_session)

    start_date = '2021-06-16'
    end_date = '2021-07-01'

    segment_counts = _get_active_info(client, start_date, end_date)


    def _prep():
        query = {
            'bool': {
                'filter': [{
                    "range": {
                        "report_date": {
                            "gte": start_date,
                            "lte": end_date,
                            'format': 'yyyy-MM-dd'
                        }
                    }
                }],
            }
        }

        for hit in query_cursor(client, query, index='reports*'):
            source = hit['_source']
            date_data = segment_counts.get(source['report_date'])
            if date_data is None:
                continue

            qualified_joins = float(source['kpi_total_joins']) - float(source['kpi_non_qualified_users'])

            source.update({
                'kpi_qualified_joins': qualified_joins,
                'kpi_active_45days_30age': date_data['45'],
                'kpi_active_45days_to_register': round(date_data['45'] / qualified_joins, 4),
                'kpi_active_60days_30age': date_data['60'],
                'kpi_active_60days_to_register': round(date_data['60'] / qualified_joins, 4),
                'kpi_active_90days_30age': date_data['90'],
                'kpi_active_90days_to_register': round(date_data['90'] / qualified_joins, 4),
            })
            yield {
                '_id': hit['_id'],
                "_type": hit['_type'],
                "_index": hit['_index'],
                "_source": {'doc': source},
                '_op_type': 'update'
            }
            # print(f"[{hit['_index']}.{hit['_id']}] {source['report_date']}: {source['kpi_total_joins']} - {source['kpi_non_qualified_users']}, {date_data['45']}, {date_data['60']}, {date_data['90']}")

    bulk(client, _prep(), index='reports-v2', doc_type='_doc')
    print('all done')
