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

    morning_begin = "09:30:00"
    morning_end = "11:30:00"
    afternoon_begin = "13:00:00"
    afternoon_end = "15:00:00"

    while tm < int(time.strftime(morning_begin, time.strptime("09:30:00", "%H:%M:%S"))):
        time.sleep(15)
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    mark = 0
    mark_tm = morning_begin

    while tm < int(time.strftime("%H%M%S", time.strptime(afternoon_end, "%H:%M:%S"))):

        if tm >= int(time.strftime("%H%M%S", time.strptime(mark_tm))):

            if mark_tm in ["10:30:00", "11:30:00", "14:00:00", "14:50:00"]:
                ####job1 小时级报告 指数小时级跟踪
                daily(trading_date, mark_tm, account, strategy_id)
                INDEX_hourly(trading_date, strategy_id)
                pass

            time.sleep(5)
            ###15分钟级程序 1 爬虫 2 分析

            if tm > int(time.strftime("%H%M%S", time.strptime(morning_end, "%H:%M:%S"))):
                mark = 0
                mark_tm = (datetime.datetime.strptime(afternoon_begin, "%H:%M:%S") + datetime.timedelta(minutes=mark*15)).strftime("%H:%M:%S")
            else:
                mark += 1
                mark_tm = (datetime.datetime.strptime(morning_begin, "%H:%M:%S") + datetime.timedelta(minutes=mark*15)).strftime("%H:%M:%S")

        while tm > int(time.strftime("%H%M%S", time.strptime(morning_end, "%H:%M:%S"))) and tm < int(time.strftime("%H%M%S", time.strptime(afternoon_begin, "%H:%M:%S"))):
            time.sleep(60)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        time.sleep(60)
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    if tm > int(time.strftime("%H%M%S", time.strptime(afternoon_begin, "%H:%M:%S"))):
        ###time out
        pass
