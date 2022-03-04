import csv

import boto3
import numpy as np

from clearvalue import app_config
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    meta_stats = ddb.query(app_config.resource_name('analytics'),
                           IndexName='GS1-index',
                           KeyConditionExpression='GS1Hash = :HashKey',
                           ExpressionAttributeValues={
                               ':HashKey': ddb.serialize_value('META_STATS'),
                           })

    utm_terms = {}
    utm_campaigns = {}
    for user in meta_stats:
        first_session_create_count = user.get('first_session_create_count', 0)
        term = user.get('utm_term', 'unknown').lower()
        all_terms = utm_terms.get(term, [])
        all_terms.append(first_session_create_count)
        utm_terms[term] = all_terms

        campaign = user.get('utm_campaign', 'unknown').lower()
        all_campaigns = utm_campaigns.get(campaign, [])
        all_campaigns.append(first_session_create_count)
        utm_campaigns[campaign] = all_campaigns

    with open('first_session_campaigns.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Campaign', 'Total', 'Avg.', 'Median'])
        for campaign in utm_campaigns:
            writer.writerow([campaign, str(len(utm_campaigns[campaign])), str(round(float(np.mean(utm_campaigns[campaign])), 2)), str(round(float(np.median(utm_campaigns[campaign])), 2))])

    with open('first_session_terms.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Term', 'Total', 'Avg.', 'Median'])
        for term in utm_terms:
            writer.writerow([term, str(len(utm_terms[term])), str(round(float(np.mean(utm_terms[term])), 2)), str(round(float(np.median(utm_terms[term])), 2))])

    print('All done')
