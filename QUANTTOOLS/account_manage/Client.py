
import logging
import strategyease_sdk
from QUANTTOOLS.account_manage.setting import yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import exceptions
import pandas as pd

def get_Client(host=yun_ip, port=yun_port, key=easytrade_password):
    logging.basicConfig(level=logging.DEBUG)
    client = strategyease_sdk.Client(host=host, port=port, key=key)
    return(client)

def get_Capital(client, account):
    res = client.get_positions(account)
    capital = float(res['sub_accounts']['可用金额'])
    return(capital)

def check_Client(client, account, strategy_id, trading_date, exceptions=exceptions, ui_log= None):
    logging.basicConfig(level=logging.DEBUG)
    try:
        QA_util_log_info(
            '##JOB Now Check Account Server ==== {}'.format(str(trading_date)), ui_log)
        account_info = client.get_account(account)
        print(account_info)
        res = client.get_positions(account)
        sub_accounts = res['sub_accounts']
        positions = res['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','成本价','市价','市值','盈亏比例(%)']]

        if exceptions is not None:
            try:
                frozen_positions = positions.set_index('证券代码').loc[exceptions]
            except:
                frozen_positions = pd.DataFrame()
        else:
            frozen_positions = pd.DataFrame()

        try:
            frozen = float(frozen_positions['市值'].sum())
        except:
            frozen = 0

        sub_accounts = sub_accounts - frozen

    except:
        send_email('错误报告', '云服务器错误,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    return(sub_accounts, frozen, positions, frozen_positions)