from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Trader.account_manage.TrackAction.BuyTrack import BuyTrack
from QUANTTOOLS.Trader.account_manage.TrackAction.SellTrack import SellTrack
from QUANTTOOLS.Market.MarketTools.trading_tools.BuildTradingFrame import build
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly

import time
import datetime

def track_roboot(target_tar, account, trading_date, percent, strategy_id,  exceptions = None):
    QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(trading_date)), ui_log = None)

    tm = int(datetime.datetime.now().strftime("%H%M%S"))

    morning_begin = "09:30:00"
    morning_end = "11:30:00"
    afternoon_begin = "13:00:00"
    afternoon_end = "15:00:00"

    while tm < int(time.strftime(morning_begin, time.strptime("09:30:00", "%H:%M:%S"))):
        time.sleep(15)
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)
    mark = 0
    mark_tm = morning_begin

    while tm < int(time.strftime("%H%M%S", time.strptime(afternoon_end, "%H:%M:%S"))):
        QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
        client = get_Client()
        sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

        QA_util_log_info('##JOB Now Build Tracking Frame ==== {}'.format(str(trading_date)), ui_log = None)
        res = build(target_tar, positions, sub_accounts, percent)

        if res is not None:
            if tm >= int(time.strftime("%H%M%S", time.strptime(mark_tm))):

                if mark_tm in ["10:30:00", "11:30:00", "14:00:00", "14:50:00"]:
                    ####job1 小时级报告 指数小时级跟踪
                    for code in res.code.tolist():
                        name = positions[positions.code == code]['证券名称']

                    if code[0:2] == '60':
                        code = 'SH' + code
                    elif code[0:3] in ['000','002','300']:
                        code = 'SZ' + code

                    res1 = stock_daily(code, trading_date, trading_date)
                    QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res[1]))
                    if res1[0] == False:
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'日线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
                    if res1[1] == False:
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'周线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
                    res2 = stock_hourly(code, trading_date, trading_date, mark_tm)
                    QA_util_log_info('{code}{name}-{trading_date}:hourly: {hourly}'.format(code=code,name=name,trading_date=trading_date,hourly=res2[0]))
                    if res2[0] == False:
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset='SELL',volume=None)
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
        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)

if __name__ == '__main__':
    pass