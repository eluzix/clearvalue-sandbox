import datetime
import time

import boto3
import isodate

import cvutils as utils
from clearvalue import app_config
from cvcore import currency_utils
from cvutils.store.keys import DBKeys
from cvcore.model.interest import amortization_table
from cvutils.dynamodb import ddb


def _account_history(uid, account_id):
    account = ddb.get_item(table_name, DBKeys.user_account(uid, account_id))
    purchase_date = account['purchase_date']
    market_value = float(account['market_value'])
    loan_id = account['loan_id']
    loan = ddb.get_item(table_name, DBKeys.user_account(uid, loan_id))
    loan_first_payment_date = utils.date_from_str(loan['loan_first_payment_date'])

    duration = isodate.parse_duration(loan['duration'])

    principal_value = float(loan['loan_original_amount'])
    loan_currency = loan.get('currency', 'USD')
    if loan_currency != 'USD':
        principal_value = currency_utils.convert_currency(principal_value, loan_currency, 'USD', on_date=loan_first_payment_date)

    monthly_payment, table = amortization_table(float(principal_value), float(loan['rate']), int(duration.months))
    loan_index = 0

    batch = []
    current_date = utils.date_from_str(purchase_date)
    last_loan_value = float(loan['loan_original_amount'])
    while current_date < utils.today():
        tp_item = account.copy()
        tp_item[DBKeys.HASH_KEY] = account_id
        tp_item[DBKeys.SORT_KEY] = DBKeys.account_time_point(current_date)

        if current_date >= loan_first_payment_date:
            if current_date.day == 1:
                last_loan_value = float(table[loan_index][2])
                loan_index += 1

        tp_item['loan_value'] = last_loan_value
        tp_item['value'] = market_value - last_loan_value

        print(tp_item[DBKeys.SORT_KEY], tp_item['loan_value'], tp_item['value'])
        batch.append(tp_item)

        current_date += datetime.timedelta(days=1)

    ddb.batch_write_items(table_name, batch)

    return batch


if __name__ == '__main__':
    tp1 = time.time()

    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')
    table_name = app_config.resource_name('accounts')

    uid = '4aaa981b-004b-4c39-a743-979ee062ddee'
    account_id = '870f074f-6aed-42db-b116-14c227d31345'

    batch = _account_history(uid, account_id)

    tp2 = time.time()
    print('*** total of {} records done in {} ***'.format(len(batch), tp2 - tp1))
