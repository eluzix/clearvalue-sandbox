import pprint

import boto3

from clearvalue import app_config
from utils import local_queries
from utils.local_queries import securities_holdings_data, cash_type_info

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    # uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'

    # prod user
    uid = 'ba01b2c4-7af8-4d12-8a11-f1d782d6f9a7'

    # demo account
    # uid = '4aaa981b-004b-4c39-a743-979ee062ddee'

    # eluzix
    # uid = '2bb40134-1a88-4491-bedf-496401a429f0'

    # eluzix in staging
    # uid = 'befba013-635c-4662-a298-ebe163c3f50c'

    # uzi@clearvalue in prod
    # uid = '7f58b698-59d7-42de-9c7d-a9e4e8d88573'

    # shaiazran@gmail in staging
    # uid = '64ae0971-45aa-4e8f-8168-d55ffba09afa'

    # shai@claritus in prod
    # uid = '673e7d2c-4508-4658-9303-0646669d65f6'

    # refaeli in dev
    # uid = '6622e97c-1dcd-47d4-9f13-17907cf4fb81'

    # pprint.pprint(asset_type_history(uid, 'loans', base_asset_type='liability'))
    # ret = mortgages_type_data(uid, tf={'timeFrame': '30days',
    #                                    'startDate': '2019-05-01',
    #                                    'endDate': '2021-04-04'
    #                                    })
    # ret = loans_type_data(uid, tf={'timeFrame': '30days',
    #                                'startDate': '2019-05-01',
    #                                'endDate': '2021-04-04'
    #                                })
    # ret = securities_transactions(uid, 'd98688f5-3854-4df1-ac47-cd3b9c0342e9')
    # ret = realestate_type_info(uid, tf={'timeFrame': '30days',
    #                                     'startDate': '2016-04-01',
    #                                     'endDate': '2021-03-23'
    #                                     })

    # ret = realestate_property_info(uid, 'e1dc4be2-04c9-448a-8f62-344ebf60fba3', tf={'timeFrame': '30days',
    #                                                                                 'startDate': '2016-04-01',
    #                                                                                 'endDate': '2021-03-23'
    #                                                                                 })

    # ret = realestate_transactions(uid, 'da08a0c2-575a-43a0-989c-4d91c5971d11', tf={'timeFrame': '90days',
    #                                                                                 'startDate': '2016-04-01',
    #                                                                                 'endDate': '2021-03-23'
    #                                                                                 })

    # ret = pe_type_info(uid)
    # ret = pe_account_info(uid, 'a03826c7-72b8-46c8-93e4-9f04f9f469b0', tf={'timeFrame': 'custom',
    #                                                                        'startDate': '2016-04-01',
    #                                                                        'endDate': '2021-03-23'
    #                                                                        })

    ret = local_queries.securities_type_info(uid, tf={'timeFrame': '30days',
                                                      'startDate': '2021-05-15',
                                                      'endDate': '2021-06-16'
                                                      })
    # ret = local_queries.securities_account_info(uid, '75dc866e-b391-4382-b0cb-2c98cb0b75f4', tf={'timeFrame': '30days',
    #                                                                                              'startDate': '2021-07-01',
    #                                                                                              'endDate': '2021-07-31'
    #                                                                                              })
    # ret = securities_holdings_data(uid, '75dc866e-b391-4382-b0cb-2c98cb0b75f4', tf={'timeFrame': '30days',
    #                                                                                 'startDate': '2016-04-01',
    #                                                                                 'endDate': '2021-03-23'
    #                                                                                 })
    # ret = home_info(uid)

    # ret = crypto_type_data(uid)
    # ret = crypto_account_info(uid, '14f55c66-a180-4e3e-a0e7-4a50def508ad')
    # ret = crypto_transactions(uid, 'ee530eb4-4cfe-475e-aded-774b905912f5', tf={'timeFrame': '30days',
    #                                    'startDate': '2021-05-15',
    #                                    'endDate': '2021-06-16'
    #                                    })

    # ret = vc_type_info(uid)
    # ret = vc_account_info(uid, '3ca59783-b57e-4995-ac20-115db7f7b4bc', tf={'timeFrame': 'custom',
    #                                                                        'startDate': '2013-01-01',
    #                                                                        'endDate': '2021-06-28'
    #                                                                        })

    # ret = cash_type_info(uid, asset_type='cash')
    # for ac in ret['cashTypeInfo']['accounts']:
    #     print(ac)
    #     print(ac['account']['accountId'], '--', ac['account']['name'], '--', ac['account']['accountMask'], '--', ac['account']['linkStatus'])
    # ret = account_info_query(uid, '0c11959d-3aca-44d4-9d5c-98c15791ea32')
    # ret = account_transactions_query(uid, '0c11959d-3aca-44d4-9d5c-98c15791ea32', tf={'timeFrame': 'custom',
    #                                                                                   'startDate': '2013-01-01',
    #                                                                                   'endDate': '2021-06-28'
    #                                                                                   })

    # ret = link_zabo_account(uid, {'id': '039c3777-ceb3-4931-b392-68acc5387fc5', 'token': 'zabosession-UwKT6CMlTEH4jqgb2K68MbX3u5dILbQsTtbG9UDHLkAEPZUnZ6pLObSGm7Asl46a', 'provider': 'celsius'})

    # ret = asset_type_info(uid, asset_type='collectable', tf={'timeFrame': 'ytd',
    #                                                          'startDate': '2013-01-01',
    #                                                          'endDate': '2021-07-19'
    #                                                          })
    # for ac in ret['loanTypeInfo']['accounts']:
    #     print(ac['account']['accountId'], '--', ac['account']['name'], '--', ac['account'].get('providerAccountId'))

    pprint.pprint(ret)
