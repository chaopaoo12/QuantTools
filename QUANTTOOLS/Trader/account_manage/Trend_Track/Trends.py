from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client


def daily(trading_date, account, strategy_id, exceptions = None):
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    positions = positions[positions['股票余额'] > 0]
    for code in positions.code.tolist():
        name = positions[positions.code == code]['证券名称']
        if code[0:2] == '60':
            code = 'SH' + code
        elif code[0:3] in ['000','002','300']:
            code = 'SZ' + code

        res = stock_daily(code,trading_date,trading_date)
        QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res[0],weekly=res[1]))
        if res[0] == False:
            send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'日线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        if res[1] == False:
            send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'周线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        res = stock_hourly(code,trading_date,trading_date)
        QA_util_log_info('{code}{name}-{trading_date}:hourly: {hourly}'.format(code=code,name=name,trading_date=trading_date,hourly=res))
        if res == False:
            send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset='SELL',volume=None)