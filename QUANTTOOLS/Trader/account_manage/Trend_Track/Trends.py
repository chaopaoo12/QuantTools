from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client


def daily(trading_date, account, strategy_id, exceptions = None):
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    positions = positions[positions['可用余额'] > 0]

    for code in list(positions.index):
        res = stock_daily(code,trading_date,trading_date)
        if res[0] is False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'日线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        if res[1] is False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'周线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        res = stock_hourly(code,trading_date,trading_date)
        if res is False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset='SELL',volume=None)