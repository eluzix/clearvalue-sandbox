import concurrent.futures
import copy
import datetime
import gzip
import json
import time

import boto3
from user_agents import parse

from clearvalue import app_config
from cvutils import boto3_client, s3_utils


def dump_s3_prefix(prefix):
    tp1 = time.time()
    bucket = app_config.resource_name('analytics')
    s3 = boto3_client('s3')
    next_continuation_token = None
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_keys = []
    batch_keys = result['Contents']
    while len(batch_keys) > 0:
        for res in batch_keys:
            all_keys.append(res["Key"])
            # print(f'{res["Key"]}')
        next_continuation_token = result.get('NextContinuationToken')
        if next_continuation_token is not None:
            result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=next_continuation_token)
            batch_keys = result['Contents']
        else:
            batch_keys = []

    print(f'Total results: {len(all_keys)}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_account = {
            executor.submit(s3_utils.save_s3_data_to_file, key, bucket, f"s3_dump/{key.replace('/', '-')}"): key
            for key in all_keys
        }

        for future in concurrent.futures.as_completed(future_to_account):
            key = future_to_account[future]
            print(f'Done saving {key}')

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def transform_file(file_path):
    exclude_keys = ['token', 'session', 'request_id', 'path', 'ts', 'log_name', 'user_agent']
    with gzip.open(file_path, 'rb') as f:
        for line in f.readlines():
            js = json.loads(line)
            if js.get('log_name') != 'client-log':
                continue

            new_event = {}
            event_pros = copy.copy(js)
            for ek in exclude_keys:
                if ek.startswith('utm_') or ek in event_pros:
                    del event_pros[ek]
            new_event['event_properties'] = event_pros

            user_props = [{k: js[k]} for k in js if k.startswith('utm_')]
            new_event['user_properties'] = user_props

            new_event['event_type'] = js['event']
            uid = js.get('uid')
            if uid is not None:
                new_event['user_id'] = uid
            # session = js.get('session')
            # if session is not None:
            #     new_event['session_id'] = session

            ua = js.get('user_agent')
            if ua is not None:
                user_agent = parse(ua)
                new_event['platform'] = user_agent.os.family
                new_event['os_name'] = user_agent.os.family
                new_event['os_version'] = user_agent.os.version_string
                new_event['device_brand'] = user_agent.device.family
                new_event['device_manufacturer'] = user_agent.device.brand

            ts = js.get('ts')
            if ts is not None:
                new_event['time'] = int(time.mktime(datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f%z').timetuple()))

            print(new_event)


def unzip_files(path):
    pass


if __name__ == '__main__':
    # INSTALL: pip3 install pyyaml ua-parser user-agents
    
    # boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # dump_s3_prefix('logs/2021/10/')
    # unzip_files('s3_dump/')
    transform_file('s3_dump/logs-2021-10-12-15-cv-prod-hose-6-2021-10-12-15-39-59-37b5a884-f4db-4c7a-a62f-7e3f82a0fc59.gz')
