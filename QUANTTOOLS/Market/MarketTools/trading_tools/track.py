from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Trader.account_manage.TrackAction.BuyTrack import BuyTrack
from QUANTTOOLS.Trader.account_manage.TrackAction.SellTrack import SellTrack
from QUANTTOOLS.Market.MarketTools.trading_tools.BuildTradingFrame import build
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import QA_fetch_stock_name
import time
import datetime

def track_roboot(target_tar, account, trading_date, percent, strategy_id,  exceptions = None):
    QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(trading_date)), ui_log = None)

    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    morning_begin = "09:30:00"
    morning_end = "11:30:00"
    afternoon_begin = "13:00:00"
    afternoon_end = "15:00:00"

    while tm < int(time.strftime("%H%M%S",time.strptime("09:30:00", "%H:%M:%S"))):
        time.sleep(15)
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)
    mark = 0
    mark_tm = "09:30:00"

    while tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):
        QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
        client = get_Client()
        sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

        QA_util_log_info('##JOB Now Build Tracking Frame ==== {}'.format(str(trading_date)), ui_log = None)

        while tm < int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):
            time.sleep(60)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        if tm >= int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):
            if mark_tm in ["09:30:00", "10:30:00", "11:30:00", "14:00:00", "14:55:00"]:
                if mark_tm == "14:55:00":
                    mark_tm = "15:00:00"
                QA_util_log_info('##JOB Now Time ==== {}'.format(str(mark_tm)), ui_log = None)

                ####job1 小时级报告 指数小时级跟踪
                for code in positions.code.tolist():
                    name = QA_fetch_stock_name(code)
                    QA_util_log_info('##JOB Now Code ==== {} {}'.format(str(code),str(name)), ui_log = None)

                    if code[0:2] == '60':
                        code = 'SH' + code
                    elif code[0:3] in ['000','002','300']:
                        code = 'SZ' + code

                    if mark_tm == "09:30:00":
                        res1 = stock_daily(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date))
                    else:
                        res1 = stock_daily(code, trading_date, trading_date)

                    QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res1[1]))

                    if mark_tm == "09:30:00":
                        res2 = stock_hourly(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date), "15:00:00")
                    else:
                        res2 = stock_hourly(code, trading_date, trading_date, mark_tm)

                    QA_util_log_info(res2, ui_log = None)
                    QA_util_log_info('{code}{name}-{trading_date}:hourly: {hourly}'.format(code=code,name=name,trading_date=trading_date,hourly=res2[0]))

                    if res2[1] == True and res2[3] <= 0:
                        ###卖出信号
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'卖出信号',direction = 'SELL',offset=mark_tm,volume=None)

                    if res2[0] == -1:
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset=mark_tm,volume=None)
                    pass
                    time.sleep(1)

                for code in list(target_tar.index):
                    name = QA_fetch_stock_name(code)
                    QA_util_log_info('##JOB Now Code ==== {} {}'.format(str(code),str(name)), ui_log = None)

                    if code[0:2] == '60':
                        code = 'SH' + code
                    elif code[0:3] in ['000','002','300']:
                        code = 'SZ' + code

                    if mark_tm == "09:30:00":
                        res1 = stock_daily(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date))
                    else:
                        res1 = stock_daily(code, trading_date, trading_date)

                    QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res1[1]))

                    if mark_tm == "09:30:00":
                        res2 = stock_hourly(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date), "15:00:00")
                    else:
                        res2 = stock_hourly(code, trading_date, trading_date, mark_tm)

                    QA_util_log_info(res2, ui_log = None)
                    QA_util_log_info('{code}{name}-{trading_date}:hourly: {hourly}'.format(code=code,name=name,trading_date=trading_date,hourly=res2[0]))

                    if res2[2] == True:
                        ###买入信号
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'买入信号',direction = 'BUY',offset=mark_tm,volume=None)

                    if res2[0] == -1:
                        send_actionnotice(strategy_id,'{code}{name}:{trading_date}'.format(code=code,name=name,trading_date=trading_date),'60min线趋势下跌',direction = 'SELL',offset=mark_tm,volume=None)
                    pass

                    time.sleep(1)

                ###15分钟级程序 1 爬虫 2 分析
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            while tm >= int(time.strftime("%H%M%S",time.strptime(morning_end, "%H:%M:%S"))) and tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_begin, "%H:%M:%S"))):

                time.sleep(600)
                tm = int(datetime.datetime.now().strftime("%H%M%S"))

            mark += 1
            if tm <= int(time.strftime("%H%M%S",time.strptime(morning_end, "%H:%M:%S"))):
                mark_tm = (datetime.datetime.strptime(morning_begin, "%H:%M:%S") + datetime.timedelta(minutes=15*mark)).strftime("%H:%M:%S")
            else:
                mark_tm = (datetime.datetime.strptime(afternoon_begin, "%H:%M:%S") + datetime.timedelta(minutes=15*(mark-8))).strftime("%H:%M:%S")

    if tm > int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):
        ###time out
        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)

if __name__ == '__main__':
    pass