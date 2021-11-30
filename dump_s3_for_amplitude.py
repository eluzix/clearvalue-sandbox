import concurrent.futures
import copy
import datetime
import gzip
import json
import os
import time

import boto3
import requests
from user_agents import parse

import cvutils
from clearvalue import app_config
from cvutils import boto3_client, s3_utils


def dump_s3_prefix(prefix, path):
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
            executor.submit(s3_utils.save_s3_data_to_file, key, bucket, f"{path}/{key.replace('/', '-')}"): key
            for key in all_keys
        }

        for future in concurrent.futures.as_completed(future_to_account):
            key = future_to_account[future]
            print(f'Done saving {key}')

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def transform_file(file_path):
    exclude_keys = ['token', 'session', 'request_id', 'path', 'ts', 'log_name', 'user_agent', 'ip', 'country']
    ret = []

    with gzip.open(file_path, 'rb') as f:
        for line in f.readlines():
            try:
                js = json.loads(line)
                if js.get('log_name') != 'client-log':
                    continue

                new_event = {}
                event_props = copy.copy(js)
                remove_keys = set()
                for ek in event_props:
                    if isinstance(event_props[ek], list):
                        remove_keys.add(ek)
                    elif ek.startswith('utm_') or ek in exclude_keys:
                        remove_keys.add(ek)

                for ek in remove_keys:
                    del event_props[ek]

                new_event['event_properties'] = event_props

                user_props = {k: js[k] for k in js if k.startswith('utm_')}
                new_event['user_properties'] = user_props

                event = js['event']
                new_event['event_type'] = event
                uid = js.get('uid')
                if uid is not None:
                    new_event['user_id'] = uid

                session = js.get('session')
                if session is not None:
                    new_event['device_id'] = session

                if uid is None and session is None:
                    # if no uid and no session ignore the record
                    continue

                ip = js.get('ip')
                if ip is not None:
                    new_event['ip'] = ip

                country = js.get('country')
                if country is not None:
                    new_event['country'] = country
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
                    new_event['time'] = int(time.mktime(datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%f%z').timetuple()))*1000

                request_id = js['request_id']
                new_event['insert_id'] = f'{request_id}:{uid}:{event.strip().replace(" ", "")}:{ts}'

                ret.append(new_event)
            except Exception as e:
                print(f'{e} for {line}')

    return ret


def process_files(path):
    tp1 = time.time()
    all_lines = []
    files_count = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            if not name.endswith('.gz'):
                continue

            files_count += 1
            lines = transform_file(os.path.join(root, name))
            all_lines.extend(lines)

    print(f'Done transforming {files_count} file with {len(all_lines)} lines')
    for chunk in cvutils.grouper(all_lines, 1900):
        chunk = [i for i in chunk if i is not None]

        print(f'Sending {len(chunk)} lines to amplitude')
        response = requests.post('https://api2.amplitude.com/batch', json={
            'api_key': app_config['amplitude']['key'],
            'events': chunk
        })

        if response.status_code != 200:
            print(f'Error sending data to amplitude, status_code: {response.status_code}, response: {response.text}')
            break
        
        # print(f'Sleeping for 1 sec')
        time.sleep(1)

    tp2 = time.time()
    print(f'All done in {tp2-tp1}')


if __name__ == '__main__':
    # INSTALL: pip3 install pyyaml ua-parser user-agents

    # boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # print(app_config['amplitude']['key'])
    # dump_s3_prefix('logs/2021/05/', 's3_dump_05')
    process_files('s3_dump_05/')
    # for l in transform_file('s3_dump/logs-2021-10-12-15-cv-prod-hose-6-2021-10-12-15-39-59-37b5a884-f4db-4c7a-a62f-7e3f82a0fc59.gz'):
    #     print(l)
