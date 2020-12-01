from QUANTTOOLS.Trader.account_manage.Trend_Track.Trends import daily
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTTOOLS.Ananlysis.Trends.setting import CN_INDEX
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
import time
import datetime

def INDEX_hourly(trading_date, hour, strategy_id):

    for code in CN_INDEX:

        res = stock_daily(code, trading_date, trading_date)
        QA_util_log_info('{code}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,trading_date=trading_date,daily=res[0],weekly=res[1]))
        if res[0] == False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'日线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        if res[1] == False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'周线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
        res = stock_hourly(code,trading_date,trading_date, hour)
        QA_util_log_info('{code}-{trading_date}:hourly: {hourly}'.format(code=code,trading_date=trading_date,hourly=res))
        if res == False:
            send_actionnotice(strategy_id,'{code}:{trading_date}'.format(code=code,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset='SELL',volume=None)


def daily_job(trading_date, account = 'name:client-1', strategy_id = '趋势跟踪'):
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("09:30:00", "%H:%M:%S")))
    target_ea = int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S")))
    target_ae = int(time.strftime("%H%M%S", time.strptime("13:00:00", "%H:%M:%S")))
    target_af = int(time.strftime("%H%M%S", time.strptime("15:00:00", "%H:%M:%S")))
    while tm < target_tm:

        while tm <= int(time.strftime("%H%M%S", time.strptime("10:30:00", "%H:%M:%S"))):
            time.sleep(15)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
        daily(trading_date, "10:30:00", account, strategy_id)

        while tm <= int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S"))):
            time.sleep(15)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
        daily(trading_date, "11:30:00", account, strategy_id)

        if tm > target_ea and tm < target_ae:
            time.sleep(600)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        while tm <= int(time.strftime("%H%M%S", time.strptime("14:00:00", "%H:%M:%S"))):
            time.sleep(15)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        daily(trading_date, "14:00:00", account, strategy_id)

        while tm <= int(time.strftime("%H%M%S", time.strptime("14:50:00", "%H:%M:%S"))):
            time.sleep(15)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
        daily(trading_date, "15:00:00", account, strategy_id)

        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        if tm > target_af:
            break

def index_job(trading_date, strategy_id = '趋势跟踪'):
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("09:30:00", "%H:%M:%S")))
    target_ea = int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S")))
    target_ae = int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S")))
    target_af = int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S")))
    while tm < target_tm:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        INDEX_hourly(trading_date, strategy_id)
        time.sleep(15)
        if tm > target_ea and tm < target_ae:
            time.sleep(600)
        if tm > target_af:
            break