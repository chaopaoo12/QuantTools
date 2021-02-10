from QUANTTOOLS.Trader.account_manage.Trend_Track.Trends import daily
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly, btc_hourly
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

def auto_btc_tracking(trading_date, strategy_id='BTC数据跟踪'):
    QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(trading_date)), ui_log = None)

    tm = int(datetime.datetime.now().strftime("%H%M%S"))

    morning_begin = "00:00:00"
    afternoon_end = "23:55:00"

    QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)
    mark = 0
    mark_tm = morning_begin

    while tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):
        QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)

        QA_util_log_info('##JOB Now Build Tracking Frame ==== {}'.format(str(trading_date)), ui_log = None)
        while tm < int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):
            time.sleep(60)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        if tm >= int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):
            if mark_tm[3:] in ["00:00", "15:00", "30:00", "45:00"]:
                QA_util_log_info('##JOB Now Time ==== {}'.format(str(mark_tm)), ui_log = None)
                ####job1 小时级报告 指数小时级跟踪
                name = 'btcbtcusd'
                QA_util_log_info('##JOB Now Code ==== {}'.format(str(name)), ui_log = None)

                res2 = btc_hourly(name, trading_date, mark_tm)
                QA_util_log_info(res2, ui_log = None)
                QA_util_log_info('{name}-{trading_date}:hourly: {hourly}'.format(name=name,trading_date=trading_date,hourly=res2[0]))
                if res2[1] == True and res2[3] <= 0:
                    ###卖出信号
                    send_actionnotice(strategy_id,'{name}:{trading_date}'.format(name=name,trading_date=trading_date),'卖出信号',direction = 'SELL',offset=mark_tm,volume=None)
                elif res2[2] == True:
                    ###买入信号
                    send_actionnotice(strategy_id,'{name}:{trading_date}'.format(name=name,trading_date=trading_date),'买入信号',direction = 'BUY',offset=mark_tm,volume=None)

                if res2[0] == -1:
                    send_actionnotice(strategy_id,'{name}:{trading_date}'.format(name=name,trading_date=trading_date),'15min线趋势下跌',direction = 'SELL',offset=mark_tm,volume=None)
                pass

            time.sleep(5)
            ###15分钟级程序 1 爬虫 2 分析
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            mark += 1
            mark_tm = (datetime.datetime.strptime(morning_begin, "%H:%M:%S") + datetime.timedelta(minutes=mark*15)).strftime("%H:%M:%S")

    if tm > int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):
        ###time out
        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)
