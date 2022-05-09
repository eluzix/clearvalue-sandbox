import sys
import boto3
import streamlit as st
import pandas as pd

sys.path.append('../../clearvalue-api/')
from cvanalytics import is_internal_user
from cvcore.store.keys import DBKeys
from cvcore.store import loaders
from cvutils.config import get_app_config

if __name__ == '__main__':
    # preferred_mfa
    boto3.setup_default_session(profile_name='clearvalue-sls')
    get_app_config().set_stage('prod')

    st.title('Starting MFA report')

    mfa_data = {}
    with st.spinner('Crunching numbers...'):
        mfa_data = {None: [3260], 'SMS': [4], 'TOTP': [10]}
        # for user in loaders.iter_users():
        #     if is_internal_user(user[DBKeys.HASH_KEY]):
        #         continue
        #
        #     mfa_type = user.get('preferred_mfa')
        #     type_data = mfa_data.get(mfa_type, 0)
        #     mfa_data[mfa_type] = type_data + 1

    # st.write(mfa_data)

    df = pd.DataFrame.from_dict(mfa_data, orient='index')
    st.table(df)
    st.bar_chart(df)
