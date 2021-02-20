
import logging
import strategyease_sdk
from QUANTTOOLS.Trader.account_manage.Setting.setting import yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Message.message_func import send_email
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_get_days_to_today
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_to_market_date
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name
import pandas as pd

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

def get_StockPos(code, client, account):
    positions = get_Position(client, account).set_index('code')
    try:
        res = float(positions.loc[code]['股票余额'])
    except:
        res = 0
    return(res)

def get_Position(client, account):
    positions = client.get_positions(account)['positions'][['证券代码','证券名称','市值','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']]
    positions=positions.astype({'市值':'float','股票余额':'float','可用余额':'float'})
    positions = positions[positions['股票余额'] > 0]
    positions['上市时间'] = positions['证券代码'].apply(lambda x:QA_util_get_days_to_today(str(QA_fetch_stock_to_market_date(x))))
    positions['INDUSTRY'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_industry(x))
    positions['NAME'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_name(x))
    positions =positions.rename(columns={'证券代码': 'code'})
    return(positions)

def check_Client(client, account, strategy_id, trading_date, exceptions, ui_log= None):
    logging.basicConfig(level=logging.DEBUG)
    try:
        QA_util_log_info(
            '##JOB Now Check Account Server ==== {}'.format(str(trading_date)), ui_log)
        res = client.get_positions(account)
    except:
        send_email('错误报告', '云服务器错误,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    try:
        QA_util_log_info('##JOB Now Get Sub_Accounts ==== {}'.format(str(trading_date)), ui_log)
        sub_accounts = float(res['sub_accounts']['总 资 产'])
    except:
        QA_util_log_info('##JOB Now Get Sub_Accounts Failed ==== {}'.format(str(trading_date)), ui_log)
        sub_accounts = 0
    try:
        QA_util_log_info('##JOB Now Get Positions ==== {}'.format(str(trading_date)), ui_log)
        positions = res['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','成本价','市价','市值','盈亏比例(%)']]
        positions=positions.astype({'市值':'float','股票余额':'float','可用余额':'float'})
        positions = positions[positions['股票余额'] > 0]
        positions['上市时间'] = positions['证券代码'].apply(lambda x:QA_util_get_days_to_today(str(QA_fetch_stock_to_market_date(x))))
        positions['INDUSTRY'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_industry(x))
        positions['NAME'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_name(x))
        positions =positions.rename(columns={'证券代码': 'code'})
    except:
        QA_util_log_info('##JOB Now Get Positions Failed ==== {}'.format(str(trading_date)), ui_log)
        positions = pd.DataFrame()
    try:
        QA_util_log_info(
            '##JOB Now Get Frozen_Positions ==== {}'.format(str(trading_date)), ui_log)
        if exceptions is not None:
            exceptions = exceptions.extend(list(positions[positions['上市时间'] <= 15].set_index('code').index))
        else:
            exceptions = list(positions[positions['上市时间'] <= 15].set_index('code').index)

        if exceptions is not None:
            try:
                frozen_positions = positions.set_index('code').loc[exceptions]
            except:
                frozen_positions = pd.DataFrame()
        else:
            frozen_positions = pd.DataFrame()
    except:
        QA_util_log_info(
            '##JOB Now Get Frozen_Positions Failed ==== {}'.format(str(trading_date)), ui_log)
        frozen_positions = pd.DataFrame()

    try:
        QA_util_log_info('##JOB Now Get Frozen ==== {}'.format(str(trading_date)), ui_log)
        frozen = float(frozen_positions['市值'].sum())
    except:
        frozen = 0

    try:
        QA_util_log_info('##JOB Now Get Holding Percent ==== {}'.format(str(trading_date)), ui_log)
        hold = float(res['sub_accounts']['股票市值'])/float(res['sub_accounts']['总 资 产'])
    except:
        hold = 0

    QA_util_log_info(
        '##JOB Now Check Account Finished ==== {}'.format(str(trading_date)), ui_log)



    return(sub_accounts, frozen, positions, frozen_positions, hold)