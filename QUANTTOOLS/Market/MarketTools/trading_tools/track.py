from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import QA_fetch_stock_name
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_UseCapital, get_StockPos, get_hold
from QUANTTOOLS.Trader.account_manage.TradAction.SELL import SELL
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_close,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_realtime
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
                for code_list in [positions.code.tolist(), list(target_tar.index)]:
                    for code in code_list:

                        name = QA_fetch_stock_name(code)
                        QA_util_log_info('##JOB Now Code ==== {} {}'.format(str(code),str(name)), ui_log = None)

                        if code[0:2] == '60':
                            code = 'SH' + code
                        elif code[0:3] in ['000','002','300']:
                            code = 'SZ' + code
                        try:
                            if mark_tm == "09:30:00":
                                res1 = stock_daily(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date))
                                QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res1[1]))
                                res2 = stock_hourly(code, QA_util_get_pre_trade_date(trading_date), QA_util_get_pre_trade_date(trading_date), "15:00:00")
                            else:
                                res1 = stock_daily(code, trading_date, trading_date)
                                QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res1[1]))
                                res2 = stock_hourly(code, trading_date, trading_date, mark_tm)

                            QA_util_log_info(res2, ui_log = None)
                            QA_util_log_info('{code}{name}-{trading_date}-{mark_tm}:hourly: {hourly}'.format(code=code,name=name,trading_date=trading_date,mark_tm=mark_tm,hourly=res2[0]))

                            if code in positions.code.tolist():
                                if res2[1] == True:
                                    ###卖出信号1
                                    send_actionnotice(strategy_id,'{code}{name}:{trading_date}-{mark_tm}'.format(code=code,name=name,trading_date=trading_date,mark_tm=mark_tm),'卖出信号',direction = 'SELL',offset=mark_tm,volume=None)

                            if code in list(target_tar.index):
                                if res2[2] == True:
                                    ###买入信号
                                    send_actionnotice(strategy_id,'{code}{name}:{trading_date}-{mark_tm}'.format(code=code,name=name,trading_date=trading_date,mark_tm=mark_tm),'买入信号',direction = 'BUY',offset=mark_tm,volume=None)
                        except:
                            pass
                        time.sleep(1)

                ###15分钟级程序 1 爬虫 2 分析
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            while tm >= int(time.strftime("%H%M%S",time.strptime(morning_end, "%H:%M:%S"))) and tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_begin, "%H:%M:%S"))) and mark_tm == "14:00:00":

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


def track_roboot2(account, trading_date, strategy_id, exceptions = None, test = False):

    QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(trading_date)), ui_log = None)

    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    morning_begin = "09:30:00"
    morning_end = "11:30:00"
    afternoon_begin = "13:00:00"
    afternoon_end = "15:00:00"

    while tm < int(time.strftime("%H%M%S",time.strptime(morning_begin, "%H:%M:%S"))):
        time.sleep(15)
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)

    while tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):
        QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
        client = get_Client()
        sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
        account_info = client.get_account(account)

        QA_util_log_info('##JOB Now Build Trading Frame ==== {}'.format(str(trading_date)), ui_log = None)

        ####job1 小时级报告 指数小时级跟踪
        for code in positions.code.tolist():
            name = QA_fetch_stock_name(code)
            hold = float(positions[positions.code==code]['成本价'])
            price = QA_fetch_get_stock_realtm_bid(code)
            close = QA_fetch_get_stock_close(code)
            high = QA_fetch_get_stock_realtime(code).high
            QA_util_log_info('##JOB Now Code {code}({name} ==== 成本:{hold} 昨收:{close} 今高:{high} 现价:{price}'.format(code=str(code),name=str(name),hold=hold,high=high, close = close, price = price), ui_log = None)
            close = price /close - 1
            hold = price /hold - 1
            high = price/high-1
            if hold <= -0.05 :
                msg = '突破开仓位-5%'
            elif close <= -0.05:
                msg = '回撤-5%'
            elif high <= -0.05:
                msg = '高点回撤-5%'
            else:
                msg = None
                ###卖出信号1
            if msg is not None:
                send_actionnotice(strategy_id,'{code}{name}:{msg}'.format(code=code,name=name, msg=msg),'卖出信号',direction = 'SELL',offset=None,volume=None)
                deal_pos = get_StockPos(code, client, account)
                target_pos = 0
                industry = positions[positions.code == code]['INDUSTRY']
                QA_util_log_info('##JOB Now Start Selling {code} ===='.format(code = code), ui_log = None)
                SELL(client, account, strategy_id, account_info, trading_date, code, name, industry, deal_pos, target_pos, target=None, close=0, type = 'end', test = True)
                time.sleep(1)
            #except:
            #        pass
        time.sleep(30)

        ###15分钟级程序 1 爬虫 2 分析
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        while tm >= int(time.strftime("%H%M%S",time.strptime(morning_end, "%H:%M:%S"))) and tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_begin, "%H:%M:%S"))):
            time.sleep(600)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            QA_util_log_info(tm)

    if tm >= int(time.strftime("%H%M%S", time.strptime(afternoon_end, "%H:%M:%S"))):
        ###time out
        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)


if __name__ == '__main__':
    pass