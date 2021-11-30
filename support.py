import json
import pprint
import time

import boto3

from clearvalue import app_config
from clearvalue.graphql import data_loaders
from clearvalue.graphql.schema import Context, api_schema
from cvcore.store.keys import DBKeys
from cvcore.store import loaders
from cvtests import DummyRequest
from cvutils import lambda_utils
from cvutils.dynamodb import ddb


def trace_accounts(uid, account_type=None, show_holdings=False, filter_symbol=None, load_active_only=True):
    accounts = loaders.load_user_accounts(uid, account_type=account_type, load_active_only=load_active_only)
    for account in accounts:
        pprint.pprint(account)
        if show_holdings:
            holdings = loaders.load_account_holdings(account['account_id'])
            if holdings is None:
                continue
            # symbols = []
            for h in holdings:
                if filter_symbol is not None and filter_symbol != h.get("symbol"):
                    continue
                # symbols.append(h.get("symbol"))
                print(f'{account["account_id"]}: {h.get("symbol")} {h.get("quantity")}, {h.get("base_cost")}, {h.get("provider_parent_id")}')
        # if len(symbols) != len(set(symbols)):
        #     print(f'for {account["account_id"]} {len(symbols)} != {len(set(symbols))}')
        print('--------------------------------------------------------')


def debug_account(uid, account_id, show_account=False, show_value=False):
    account = ddb.get_item(app_config.resource_name('accounts'), DBKeys.user_account(uid, account_id))
    if show_account:
        pprint.pprint(account)

    if show_value:
        print(f"account value: {account['value']}")

    holdings = loaders.load_account_holdings(account_id)
    for h in holdings:
        print(f'{h.get("symbol")} ==> Q: {h.get("quantity")}, V: {h.get("value")}')


def execute_gql_query(uid, query, variables, operation_name):
    request = DummyRequest()
    context = Context(request=request)

    context.request.user['uid'] = uid
    context.loaders['Institution'] = data_loaders.InstitutionLoader()

    result = api_schema.execute(query,
                                context=context,
                                operation_name=operation_name,
                                variable_values=variables,
                                )
    if result.errors:
        errors = [str(e) for e in result.errors]
        print(f'[{operation_name}] error: {errors}')
        raise Exception(errors)

    return result.data


def deactivate_user_account(uid):
    query = """
    mutation DeactivateProfile{
         deactivateProfile {
             success
         }
      }"""

    data = execute_gql_query(uid, query, {}, 'DeactivateProfile')
    pprint.pprint(data)


def invoke_process_account(uid):
    ret = lambda_utils.invoke({'uid': uid}, 'yodlee.process')
    print(ret)


def delete_hash_key(hkey):
    tp1 = time.time()
    table_name = app_config.resource_name('accounts')
    keys = ddb.query(table_name, KeyConditionExpression='HashKey = :HashKey',
                     ProjectionExpression='HashKey, SortKey',
                     ExpressionAttributeValues={
                         ':HashKey': ddb.serialize_value(hkey),
                     })
    batch = [{DBKeys.HASH_KEY: i[DBKeys.HASH_KEY], DBKeys.SORT_KEY: i[DBKeys.SORT_KEY]} for i in keys]
    if len(batch) > 0:
        print(f'About to delete {len(batch)} keys')
        ddb.batch_delete_items(table_name, batch)

    tp2 = time.time()
    print(f'All done in {tp2 - tp1}')


def update_user_account(uid, account_id, item, fields):
    return ddb.update_with_fields(app_config.resource_name('accounts'),
                                  DBKeys.user_account(uid, account_id),
                                  item, fields)


# ----------------------------
def yodlee_transaction_history(uid):
    # data = lambda_utils.invoke({'uid': uid, 'action': 'tr-history', 'from_date': '2021-09-01'}, 'yodlee.prod_sandbox')
    # with open('/Users/uzix/Downloads/tr_his.json', 'w') as f:
    #     f.write(data)

    with open('/Users/uzix/Downloads/tr_his.json', 'r') as f:
        data = json.load(f)

    for tr in data:
        print(tr)


def yodlee_support(uid):
    # data = lambda_utils.invoke({'uid': uid, 'action': 'history', 'account_id': 12147902, 'from_date': '2021-01-01', 'to_date': '2021-05-01'}, 'yodlee.prod_sandbox')

    # data = lambda_utils.invoke({'uid': uid, 'norm': False}, 'yodlee.prod_sandbox')
    # with open('/Users/uzix/Downloads/chas.json', 'w') as f:
    #     f.write(data)

    with open('/Users/uzix/Downloads/chas.json', 'r') as f:
        data = f.read()

    data = json.loads(data)
    # # # print(da/ta)
    accounts = data.get('accounts')
    # # pprint.pprint(accounts)
    # print('------------ accounts ------------')
    for account in accounts:
        # if account['CONTAINER'] == 'loan' and account['accountType'] == 'MORTGAGE':
        # if account['CONTAINER'] == 'loan':
        # if account['providerId'] == '9565':
        # if account['id'] in [14302795, 14302794]:
            # if account['providerName'] == 'E*TRADE':
            # if account['accountName'] == 'Auto Used Fixed':
            #     print(f"For account {account['accountName']}@{account['providerName']}, nextUpdateScheduled: {account['dataset'][0].get('nextUpdateScheduled')}, dataset: {account['dataset']}")
            pprint.pprint(account)
            # if account['dataset'][0].get('nextUpdateScheduled') is None:
            #     pprint.pprint(account)
            # print(account['dataset'][0]['updateEligibility'], account['dataset'][0].get('lastUpdated'), account['dataset'][0].get('nextUpdateScheduled'))
    # # #         print(yodlee.normalize_account(account))
    # # # #     if account['name'].startswith('CAITLIN R KLEIN CAPITAL MGMT'):
    # # # #         print(account)

    # print('------------ holdings ------------')
    # holdings = data.get('holdings')
    # # # # # total = 0
    # for h in holdings:
    #     # if h.get('providerAccountId') == 12793061:
    #     if h.get('accountId') in [14302795, 14302794]:
    #         pprint.pprint(h)
    # #         ht = h.get('quantity', 0) * h.get('price', {}).get('amount', 0)
    # #         total += ht
    # #         # print(f"{h.get('symbol')} == {ht}")
    # #         print(yodlee._normalize_holding(h))
    # # print(f'** done, total value {total} **')

    # print('------------ transactions ------------')
    # transactions = data.get('transactions')
    # transactions.sort(key=lambda t: t.get('transactionDate', ''))
    # # # total = 0
    # for t in transactions:
    #     if t.get('accountId') == 14108440:
    #         print(t)
    # # #     pprint.pprint(t['amount']['amount'])


# ----------------------------


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')

    # account_id = '4de0a50c-823f-41db-89ca-5bf77b43699e'
    # debug_account(uid, account_id, show_value=True)
    #
    # deactivate_user_account('2e9efa58-e9d9-4348-aa34-6de239337ad4')

    # account_id = '532f0b9a-25d0-4282-b735-a94678ddf330'
    # delete_hash_key('3e38b778-89eb-47ad-918a-865b80ea3bf0')

    # production user
    uid = 'ba01b2c4-7af8-4d12-8a11-f1d782d6f9a7'

    # demo account
    # uid = '4aaa981b-004b-4c39-a743-979ee062ddee'

    # eluzix
    # uid = '2bb40134-1a88-4491-bedf-496401a429f0'

    # print(update_user_account(uid, '23a55638-1548-4f48-b98d-fecf994fbdf4', {
    #     'account_type': 'sp',
    #     'original_account_type': 'cash'
    # }, ['account_type', 'original_account_type']))

    yodlee_support(uid)
    # yodlee_transaction_history(uid)
