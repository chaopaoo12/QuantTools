
import logging
import strategyease_sdk
from QUANTTOOLS.account_manage.setting import yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import QA_util_log_info,QA_util_today_str
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import exceptions
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_to_market_date
import pandas as pd
import datetime

def date_func(date):
    if (date is None) or date in ['None', 0, '0']:
        d2 = datetime.datetime.strptime(QA_util_today_str(),"%Y-%m-%d")
    else:
        d2=datetime.datetime.strptime(date,"%Y%m%d")
    d1 = datetime.datetime.strptime(QA_util_today_str(),"%Y-%m-%d")
    diff_days=d1-d2
    return(diff_days.days)

def get_Client(host=yun_ip, port=yun_port, key=easytrade_password):
    logging.basicConfig(level=logging.DEBUG)
    client = strategyease_sdk.Client(host=host, port=port, key=key)
    return(client)

def get_UseCapital(client, account):
    res = client.get_positions(account)
    capital = float(res['sub_accounts']['可用金额'])
    return(capital)

def get_AllCapital(client, account):
    res = client.get_positions(account)
    capital = float(res['sub_accounts']['总 资 产'])
    return(capital)

def get_Position(client, account):
    positions = client.get_positions(account)['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']]
    positions = positions[positions['股票余额'].astype(float) > 0]
    positions['上市时间'] = positions['证券代码'].apply(lambda x:date_func(str(QA_fetch_stock_to_market_date(x))))
    return(positions)

def check_Client(client, account, strategy_id, trading_date, exceptions=exceptions, ui_log= None):
    logging.basicConfig(level=logging.DEBUG)
    try:
        QA_util_log_info(
            '##JOB Now Check Account Server ==== {}'.format(str(trading_date)), ui_log)
        account_info = client.get_account(account)
        print(account_info)
        res = client.get_positions(account)
        sub_accounts = float(res['sub_accounts']['总 资 产'])
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

        #sub_accounts = sub_accounts - frozen

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