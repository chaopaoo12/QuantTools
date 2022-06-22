import logging
import strategyease_sdk
import easytrader
from QUANTTOOLS.Trader.account_manage.Setting.setting import yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Message.message_func import send_email
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_get_days_to_today
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_to_market_date
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name,QA_fetch_stock_industryinfo
import pandas as pd
import gmtrade.api

class Proxy_client():

    def __init__(self, client, type, **kwargs):
        self.client = client
        self.type = type

def get_Client(type='yun_ease',**kwargs):
    if type == 'yun_ease':
        logging.basicConfig(level=logging.DEBUG)
        client = strategyease_sdk.Client(host=kwargs['host'], port=kwargs['port'], key=kwargs['key'])
        client = Proxy_client(client, type=type)
        QA_util_log_info(
            '##JOB Now Get Account Server Online')
    elif type == 'local':
        client = easytrader.use("ths")
        client.connect(trader_path=kwargs['trader_path'])
        client = Proxy_client(client, type=type)
        QA_util_log_info(
            '##JOB Now Get Account Server Local')
    elif type == 'sim_myquant':
        gmtrade.api.set_token(kwargs['token'])
        gmtrade.api.set_endpoint(kwargs['server'])
        account = gmtrade.api.account(account_id=kwargs['account'], account_alias=kwargs['name'])
        gmtrade.api.login(account)
        client = Proxy_client(account, type=type)
    else:
        QA_util_log_info(
            '##JOB Now Get Account Server Failed')
    return(client)

def get_UseCapital(client, account=None):
    if client.type == 'yun_ease':
        res = client.client.get_positions(account)
        capital = float(res['sub_accounts']['可用金额'])
        return(capital)
    elif client.type == 'local':
        capital = client.client.balance['资金余额']
        return(capital)
    elif client.type == 'sim_myquant':
        return(gmtrade.api.get_cash(client.client).available)
    else:
        pass

def get_AllCapital(client, account=None):
    if client.type == 'yun_ease':
        res = client.client.get_positions(account)
        capital = float(res['sub_accounts']['可用金额'])
        return(capital)
    elif client.type == 'local':
        capital = client.client.balance['总资产']
        return(capital)
    elif client.type == 'sim_myquant':
        return(gmtrade.api.get_cash(client.client).nav)
    else:
        pass

def get_Position(client, account=None):
    if client.type == 'yun_ease':
        positions = pd.DataFrame(client.position)
    elif client.type == 'online':
        positions = client.get_positions(account)['positions'][['证券代码','证券名称','市值','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']]
    elif client.type == 'sim_myquant':
        positions = gmtrade.api.get_positions(client.client)

        if len(positions) > 0:
            symbol = []
            amount = []
            volume = []
            available = []
            volume_today = []
            fpnl = []
            fpnl_per = []


            for item in positions:
                symbol.append(item.symbol[5:])
                amount.append(item.amount)
                volume.append(item.volume)
                available.append(item.volume-item.volume_today)
                volume_today.append(item.volume_today)
                fpnl.append(item.fpnl)
                fpnl_per.append(item.fpnl/item.amount)

            positions = pd.DataFrame({'证券代码':symbol,'证券名称':None,'市值':amount, '股票余额':volume, '可用余额':available, '冻结数量':volume_today, '参考盈亏':fpnl, '盈亏比例(%)':fpnl_per})

    positions=positions.astype({'市值':'float','股票余额':'float','可用余额':'float'})
    positions = positions[positions['股票余额'] > 0]
    positions['上市时间'] = positions['证券代码'].apply(lambda x:QA_util_get_days_to_today(str(QA_fetch_stock_to_market_date(x))))
    positions['INDUSTRY'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_industryinfo(x).SWHY.values[0])
    positions['NAME'] = positions['证券代码'].apply(lambda x:QA_fetch_stock_name(x).values[0])
    positions = positions.rename(columns={'证券代码': 'code'})
    return(positions)

def get_hold(client, account=None):
    hold = 1 - get_UseCapital(client, account)/get_AllCapital(client, account)
    return(hold)

def get_StockPos(code, client, account=None):
    positions = get_Position(client, account).set_index('code')
    try:
        res = float(positions.loc[code]['股票余额'])
    except:
        res = 0
    return(res)

def get_StockCapital(code, client, account=None):
    positions = get_Position(client, account).set_index('code')
    try:
        res = float(positions.loc[code]['市值'])
    except:
        res = 0
    return(res)

def get_StockHold(code, client, account=None):
    positions = get_Position(client, account).set_index('code')
    try:
        res = float(positions.loc[code]['可用余额'])
    except:
        res = 0
    return(res)


def check_Client(client, account, strategy_id, trading_date, exceptions, ui_log= None):
    logging.basicConfig(level=logging.DEBUG)
    try:
        QA_util_log_info(
            '##JOB Now Check Account Server ==== {}'.format(str(trading_date)), ui_log)
        QA_util_log_info('##JOB Now Get Sub_Accounts ==== {}'.format(str(trading_date)), ui_log)
        sub_accounts = get_AllCapital(client, account)

    except:
        QA_util_log_info('##JOB Now Get Sub_Accounts Failed ==== {}'.format(str(trading_date)), ui_log)
        sub_accounts = 0
        send_email('错误报告', '云服务器错误,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    try:
        QA_util_log_info('##JOB Now Get Positions ==== {}'.format(str(trading_date)), ui_log)
        positions = get_Position(client, account)
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

    QA_util_log_info(
        '##JOB Now Check Account Finished ==== {}'.format(str(trading_date)), ui_log)

    return(sub_accounts, frozen, positions, frozen_positions)