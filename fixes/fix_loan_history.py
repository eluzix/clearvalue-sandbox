import json
import time

import boto3
import isodate

import cvutils as utils
from clearvalue import app_config
from clearvalue.graphql.schema.loans import _rebuild_loan_dates
from cvcore.store import loaders
from cvcore.store.keys import DBKeys
from cvcore.model import interest
from cvcore.model.cv_types import AccountTypes
from cvutils import cognito_utils
from cvutils.dynamodb import ddb


def _loan_history(uid, account_id):
    table_name = app_config.resource_name('accounts')
    loan = ddb.get_item(table_name, DBKeys.user_account(uid, account_id))
    creation_date = utils.date_from_str(loan['loan_origination_date'])
    payment_date = utils.date_from_str(loan['loan_first_payment_date'])
    principal_value = float(loan['loan_original_amount'])
    duration = isodate.parse_duration(loan['duration'])
    rate = float(loan['rate'])

    monthly_payment, table = interest.amortization_table(principal_value, float(rate), int(duration.months))

    batch = _rebuild_loan_dates(loan, creation_date, payment_date, table)

    return batch


def _user_currency_loans(uid):
    tp1 = time.time()
    table_name = app_config.resource_name('accounts')
    loans = loaders.load_user_accounts(uid, account_type=AccountTypes.LOAN)
    total_records = 0
    for loan in loans:
        if loan.get('currency', 'USD') != 'USD':
            batch = _loan_history(uid, loan['account_id'])
            print('For {}@{} saving {} records'.format(uid, loan['account_id'], len(batch)))
            total_records += len(batch)
            # ddb.batch_write_items(table_name, batch)

    tp2 = time.time()
    print('*** total of {} records done in {} ***'.format(total_records, tp2 - tp1))


def _fix_single_user_loan():
    tp1 = time.time()

    table_name = app_config.resource_name('accounts')

    uid = '4aaa981b-004b-4c39-a743-979ee062ddee'
    # account_id = '4d718462-017a-4412-8003-1f4711672859'
    # account_id = 'dd927e26-4f21-4cf9-96cd-dc21a8f68db6'
    account_id = '1530998d-1a75-4fe8-85ea-95061d4544e9'

    # _user_currency_loans(uid)
    # batch = []

    batch = _loan_history(uid, account_id)
    for item in batch:
        print(item[DBKeys.SORT_KEY], '=', item['value'])
    #
    print('Saving...')
    # ddb.batch_write_items(table_name, batch)

    tp2 = time.time()
    print('*** total of {} records done in {} ***'.format(len(batch), tp2 - tp1))


def _fix_users_currency_loans():
    boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    app_config.set_stage('staging')

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        _user_currency_loans(uid)


def _dump_all_loans():
    all_loans = []
    last_size = 0

    for user in loaders.iter_users():
        uid = user[DBKeys.HASH_KEY]

        try:
            loans = loaders.load_user_accounts(uid, account_type=AccountTypes.LOAN)
            for loan in loans:
                if loan.get('is_manual') is True:
                    print(f'for {uid} saving {loan["account_id"]}')
                    all_loans.append({'uid': uid, 'account_id': loan['account_id'], 'property_id': loan.get('property_id')})
        except Exception as e:
            pass

        if len(all_loans) != last_size:
            with open('all_loans.json', 'w') as fout:
                json.dump(all_loans, fout)
        last_size = len(all_loans)

    print('All Done')


def _check_loan_value():
    table = app_config.resource_name('accounts')
    with open('all_loans.json', 'r') as fin:
        js = json.load(fin)

    new_loans = []
    for loan in js:
        value1 = ddb.get_item(table, DBKeys.hash_sort(loan['account_id'], DBKeys.account_time_point('2021-07-31')))
        if value1 is None:
            continue
        value2 = ddb.get_item(table, DBKeys.hash_sort(loan['account_id'], DBKeys.account_time_point('2021-08-01')))
        if value2 is None:
            continue

        value1 = value1.get('value')
        value2 = value2.get('value')
        if value1 is None or value2 is None:
            continue

        if value2 > value1:
            print(f"Adding {loan['uid']}/{loan['account_id']} for {value1} and {value2}")
            new_loans.append(loan)

    print(f'from {len(js)} need to fix {len(new_loans)} loans')
    with open('need_fix_loan.json', 'w') as fout:
        json.dump(new_loans, fout)

    print('all done')


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    # _fix_single_user_loan()

    # _user_currency_loans('79bea3db-b638-4ec7-9c74-7fdebb1afae2')
    # _fix_users_currency_loans()

    # _dump_all_loans()
    _check_loan_value()
