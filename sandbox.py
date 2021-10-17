import datetime

import boto3
import pytz

from clearvalue import app_config
from clearvalue.lib import utils
from clearvalue.lib.dynamodb import ddb
from clearvalue.lib.store import DBKeys


def day_of_interest(dt: datetime.datetime, day_of_interest: int) -> int:
    first_of_month = datetime.datetime(dt.year, dt.month, 1, tzinfo=pytz.utc)
    interest_date = first_of_month + datetime.timedelta(days=day_of_interest - 1)

    if dt.month == interest_date.month:
        return day_of_interest

    # if interest day isn't in the month return last day of month
    last_of_month = interest_date - datetime.timedelta(days=interest_date.day)
    return last_of_month.day


def w():
    all_errors = {}
    error_type = 'action:{action_name}'
    existing_error = all_errors.get(error_type)
    if existing_error is None:
        all_errors[error_type] = 'only 1 error message here'
    else:
        all_errors[error_type] = 'general error message here'


if __name__ == '__main__':
    for g,e in zip(utils.grouper([1,2,3,4,5,6,7], 3), utils.grouper(['a', 'b', 'c', 'd', 'e'], 3)):
        print(g)
        print(e)
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')
    #
    # uid = 'd67d6dda-4e91-4f5b-a9a5-d33ca5db606c'
    # account_id = 'e9327161-80e5-47c7-a469-9aba4f8584d2'
    #
    # table = app_config.resource_name('accounts')
    # sd = datetime.datetime(2021, 7, 31, tzinfo=pytz.utc)
    # ed = datetime.datetime(2021, 8, 9, tzinfo=pytz.utc)
    # start_value = 309857.36
    # end_value = 311723.20191
    # daily_diff = round((end_value-start_value)/9, 2)
    # last_value = start_value
    # while sd <= ed:
    #     rd_str = utils.date_to_str(sd)
    #     print(f'For {rd_str} value = {last_value}')
    #     ddb.update_with_fields(table, DBKeys.hash_sort(account_id, DBKeys.account_time_point(rd_str)), {'value': last_value}, ['value'])
    #     last_value += daily_diff
    #     sd = sd + datetime.timedelta(days=1)
