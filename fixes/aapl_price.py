import boto3

from clearvalue import app_config
from clearvalue.lib import cognito_utils, utils
from clearvalue.lib.store import loaders
from clearvalue.model.cv_types import AccountTypes
from utils.users_utils import calc_user_daily


def fix_user(uid, fix_date):
    dt_fix_date = utils.date_from_str(fix_date)
    accounts = loaders.load_user_accounts(uid, account_type=AccountTypes.SECURITIES_PORTFOLIO, for_date=fix_date)
    for ac in accounts:
        if ac.get('is_high_level') is True:
            continue

        account_id = ac['account_id']
        holdings = loaders.load_account_holdings(account_id, for_date=dt_fix_date)
        if holdings is None:
            continue

        for h in holdings:
            if h.get('symbol', '').lower() == 'aapl':
                print(f'found AAPL holding for {uid}')
                calc_user_daily(uid, run_for=fix_date)
                return


if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    fix_date = '2020-11-30'

    for user in cognito_utils.iterate_users():
        uid = cognito_utils.uid_from_user(user)
        fix_user(uid, fix_date)
