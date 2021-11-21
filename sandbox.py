import datetime

import pytz

from clearvalue import app_config
from cvcore.store import DBKeys
from cvutils.dynamodb import ddb


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
    # boto_session = boto3.session.Session(profile_name='clearvalue-sls')
    # boto3.setup_default_session(profile_name='clearvalue-sls')
    # app_config.set_stage('prod')

    ddb.collect_query_data = True
    ret = ddb.get_item(app_config.resource_name('accounts'), DBKeys.info_key('f417d7b5-cb66-4ef1-a36a-9c6806d0af0f'))
    # print(ret)
