import pprint
import time

import boto3

from clearvalue import app_config
from utils import local_queries

if __name__ == '__main__':
    boto3.setup_default_session(profile_name='clearvalue-sls')
    app_config.set_stage('prod')

    # boto3.setup_default_session(profile_name='clearvalue-stage-sls')
    # app_config.set_stage('staging')
    # uid = '80668c5e-84a9-479f-b969-1c5bd51b5932'

    # prod user
    uid = '4e84de49-e416-4d0e-8919-e9de5eadfc9e'

    # demo account
    # uid = '4aaa981b-004b-4c39-a743-979ee062ddee'

    # eluzix
    # uid = '2bb40134-1a88-4491-bedf-496401a429f0'

    # eluzix in staging
    # uid = 'befba013-635c-4662-a298-ebe163c3f50c'

    # uzi@clearvalue in prod
    # uid = '7f58b698-59d7-42de-9c7d-a9e4e8d88573'

    # shaiazran@gmail in staging
    # uid = '3e992797-4ab3-438b-977c-f6eb8b7ffcd5'

    # shai@claritus in prod
    # uid = '673e7d2c-4508-4658-9303-0646669d65f6'

    # refaeli in dev
    # uid = '6622e97c-1dcd-47d4-9f13-17907cf4fb81'

    tp1 = time.time()
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

    # ret = local_queries.pe_type_info(uid)
    # ret = pe_account_info(uid, 'ba940597-9570-4fa6-b629-3ab700f0a5c2', tf={'timeFrame': '30days',
    #                                                                        'startDate': '2016-04-01',
    #                                                                        'endDate': '2021-03-23'
    #                                                                        })

    ret = local_queries.securities_type_info(uid, tf={'timeFrame': '30days',
                                                      'startDate': '2021-05-15',
                                                      'endDate': '2021-06-16'
                                                      })
    # ret = local_queries.securities_account_info(uid, 'e06ff549-67c3-4db8-a97e-587044b23e33', tf={'timeFrame': '90days',
    #                                                                                              'startDate': '2021-07-01',
    #                                                                                              'endDate': '2021-07-31'
    #                                                                                              })
    # ret = local_queries.securities_holdings_data(uid, 'e06ff549-67c3-4db8-a97e-587044b23e33', tf={'timeFrame': '30days',
    #                                                                                 'startDate': '2016-04-01',
    #                                                                                 'endDate': '2021-03-23'
    #                                                                                 })
    # ret = local_queries.home_info(uid, tf={'timeFrame': '30days',
    #                                        'startDate': '2012-01-01',
    #                                        'endDate': '2021-03-23'
    #                                        })

    # ret = local_queries.crypto_type_data(uid)
    # ret = local_queries.crypto_account_info(uid, '7ec5a023-f559-4205-a84e-234165a71972')
    # ret = crypto_transactions(uid, 'ee530eb4-4cfe-475e-aded-774b905912f5', tf={'timeFrame': '30days',
    #                                    'startDate': '2021-05-15',
    #                                    'endDate': '2021-06-16'
    #                                    })

    # ret = local_queries.vc_type_info(uid)
    # ret = vc_account_info(uid, '3ca59783-b57e-4995-ac20-115db7f7b4bc', tf={'timeFrame': 'custom',
    #                                                                        'startDate': '2013-01-01',
    #                                                                        'endDate': '2021-06-28'
    #                                                                        })

    # ret = local_queries.cash_type_info(uid, asset_type='cash', tf={'timeFrame': '30days',
    #                                                                'startDate': '2021-05-15',
    #                                                                'endDate': '2021-06-16'
    #                                                                })
    # for ac in ret['cashTypeInfo']['accounts']:
    #     print(ac)
    #     print(ac['account']['accountId'], '--', ac['account']['name'], '--', ac['account']['accountMask'], '--', ac['account']['linkStatus'])
    # ret = account_info_query(uid, '0c11959d-3aca-44d4-9d5c-98c15791ea32')
    # ret = local_queries.account_transactions_query(uid, '8c9e85dc-c455-4619-bd42-177f7dd2afa2', tf={'timeFrame': 'max',
    #                                                                                                 'startDate': '2021-08-05',
    #                                                                                                 'endDate': '2021-12-23'
    #                                                                                                 })

    # ret = link_zabo_account(uid, {'id': '039c3777-ceb3-4931-b392-68acc5387fc5', 'token': 'zabosession-UwKT6CMlTEH4jqgb2K68MbX3u5dILbQsTtbG9UDHLkAEPZUnZ6pLObSGm7Asl46a', 'provider': 'celsius'})

    # ret = asset_type_info(uid, asset_type='collectable', tf={'timeFrame': 'ytd',
    #                                                          'startDate': '2013-01-01',
    #                                                          'endDate': '2021-07-19'
    #                                                          })
    # for ac in ret['loanTypeInfo']['accounts']:
    #     print(ac['account']['accountId'], '--', ac['account']['name'], '--', ac['account'].get('providerAccountId'))

    # ret = local_queries.all_assets_query(uid, tf={'timeFrame': '30days',
    #                                               'startDate': '2021-05-15',
    #                                               'endDate': '2021-06-16'
    #                                               })
    # ret = local_queries.tag_info_query(uid, 'loan', tf={'timeFrame': 'custom',
    #                                               'startDate': '2000-01-01',
    #                                               'endDate': '2000-01-03'
    #                                               })
    tp2 = time.time()
    print(f'-------------------------> {tp2 - tp1}')
    pprint.pprint(ret)
    print(f'-------------------------> {tp2 - tp1}')
