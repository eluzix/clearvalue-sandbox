import time

from clearvalue import app_config
from cvcore.store.keys import DBKeys
from cvutils.dynamodb import ddb

if __name__ == '__main__':
    tp1 = time.time()
    from_user = 'e18020b7-3e7b-4729-9cca-601f85689d48'
    to_user = 'c35fb33a-546e-4a66-adc0-1774572de7c7'
    table_name = app_config.resource_name('accounts')

    items = ddb.query(table_name, KeyConditionExpression='HashKey = :HashKey', ExpressionAttributeValues={
        ':HashKey': ddb.serialize_value(from_user),

    })

    batch = []
    for item in items:
        status = item.get('account_status')
        if status is None:
            status = item.get('status', 'ok')

        if status.lower() == 'deleted':
            continue

        item[DBKeys.HASH_KEY] = to_user
        item['uid'] = to_user
        batch.append(item)

    ddb.batch_write_items(table_name, batch)

    tp2 = time.time()
    print('*** done saving {} items in {}'.format(len(batch), tp2-tp1))
