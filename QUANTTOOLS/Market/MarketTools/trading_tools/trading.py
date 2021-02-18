from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Trader.account_manage.TradAction.BUY import BUY
from QUANTTOOLS.Trader.account_manage.TradAction.SELL import SELL
from QUANTTOOLS.Trader.account_manage.TradAction.HOLD import HOLD
from QUANTTOOLS.Market.MarketTools.trading_tools.BuildTradingFrame import build
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_realtm_ask
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import QA_fetch_stock_name
import time
import datetime

def trade_roboot(target_tar, account, trading_date, percent, strategy_id, type='end', exceptions = None, test = False):

    QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
    client = get_Client()
    QA_util_log_info('##JOB Now Cancel Orders ===== {}'.format(str(trading_date)), ui_log = None)
    client.cancel_all(account)
    QA_util_log_info(target_tar, ui_log = None)
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
    account_info = client.get_account(account)

    if target_tar is None:
        QA_util_log_info('触发清仓 ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'触发清仓:{}'.format(trading_date),'触发清仓',direction = 'SELL',offset='SELL',volume=None)
        #e = send_trading_message(account, strategy_id, account_info, None, "触发清仓", None, 0, direction = 'SELL', type='MARKET', priceType=4,price=None, client=client)

    QA_util_log_info('##JOB Now Build Trading Frame ===== {}'.format(str(trading_date)), ui_log = None)
    res = build(target_tar, positions, sub_accounts, percent)
    res1 = res
    QA_util_log_info(res[['NAME','INDUSTRY','deal','close','目标持股数','股票余额','可用余额','冻结数量']])

    send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'开始交易',direction = 'HOLD',offset='HOLD',volume=None)

    while res.deal.apply(abs).sum() > 0:

        QA_util_log_info('##JOB Now Start Trading ===== {}'.format(str(trading_date)), ui_log = None)

        QA_util_log_info('##JOB Now Check Timing ===== {}'.format(str(trading_date)), ui_log = None)

        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        target_ea = int(time.strftime("%H%M%S", time.strptime("09:25:00", "%H:%M:%S")))
        target_af = int(time.strftime("%H%M%S", time.strptime("15:00:00", "%H:%M:%S")))

        if tm >= target_af:
            QA_util_log_info('已过交易时段 {hour} ==================== {date}'.format(hour = tm, date = trading_date), ui_log=None)
            send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'已过交易时段',direction = 'HOLD',offset='HOLD',volume=None)
            if test == False:
                break
        #if tm >= target_af:
        #    break

        QA_util_log_info('##JOB Now Start Selling ===== {}'.format(str(trading_date)), ui_log = None)
        if res[res['deal']<0].shape[0] == 0:
            QA_util_log_info('无卖出动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] < 0].index:
                QA_util_log_info('##JOB Now Prepare Selling {code} Info ==== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                target_pos = float(res.loc[code]['目标持股数'])
                target = float(res.loc[code]['股票余额'])
                name = res.loc[code]['NAME']
                industry = res.loc[code]['INDUSTRY']
                deal_pos = abs(float(res.loc[code]['deal']))
                close = float(res.loc[code]['close'])

                QA_util_log_info('##JOB Now Start Selling {code} ==== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                SELL(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type = type, test = test)

                time.sleep(3)

            time.sleep(15)

        QA_util_log_info('##JOB Now Start Holding ===== {}'.format(str(trading_date)), ui_log = None)
        if res[res['deal'] == 0].shape[0] == 0:
            QA_util_log_info('无持续持仓动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] == 0].index:
                QA_util_log_info('##JOB Now Prepare Holding {code} Info ===== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                target_pos = float(res.loc[code]['目标持股数'])
                target = float(res.loc[code]['股票余额'])
                name = res.loc[code]['NAME']
                industry = res.loc[code]['INDUSTRY']
                deal_pos = abs(float(res.loc[code]['deal']))
                close = float(res.loc[code]['close'])

                QA_util_log_info('##JOB Now Start Holding {code} ===== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                HOLD(strategy_id, account_info,trading_date, code, name, industry, target_pos, target)

                time.sleep(1)

        QA_util_log_info('##JOB Now Start Buying ===== {}'.format(str(trading_date)), ui_log = None)
        if res[res['deal'] > 0].shape[0] == 0:
            QA_util_log_info('无买入动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] > 0].index:
                QA_util_log_info('##JOB Now Prepare Buying {code} Info ===== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                target_pos = float(res.loc[code]['目标持股数'])
                target = float(res.loc[code]['股票余额'])
                name = res.loc[code]['NAME']
                industry = res.loc[code]['INDUSTRY']
                deal_pos = abs(float(res.loc[code]['deal']))
                close = float(res.loc[code]['close'])

                QA_util_log_info('##JOB Now Start Buying {code} ===== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                BUY(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type, test = test)

                time.sleep(3)

            time.sleep(30)

        if type == 'end':
            QA_util_log_info('##JOB Now Cancel Orders ===== {}'.format(str(trading_date)), ui_log = None)
            client.cancel_all(account)

            QA_util_log_info('##JOB Now Refresh Account Info ==== {}'.format(str(trading_date)), ui_log = None)
            sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
            sub_accounts = sub_accounts - frozen

            QA_util_log_info('##JOB Now ReBuild Trading Frame ==== {}'.format(str(trading_date)), ui_log = None)
            res = build(target_tar, positions, sub_accounts, percent)

        elif type == 'morning':
            QA_util_log_info('##JOB Now Morning Trading Success ==== {}'.format(str(trading_date)), ui_log = None)
            break
        else:
            QA_util_log_info('##Trading type must in [end, morning] ==== {}'.format(str(trading_date)), ui_log = None)
            break

    QA_util_log_info('交易完成 ==================== {}'.format(trading_date), ui_log=None)
    send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'交易完成',direction = 'HOLD',offset='HOLD',volume=None)

    return(res1)


def trade_roboot2(target_tar, account, trading_date, percent, strategy_id, type='end', exceptions = None, test = False):

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
        account_info = client.get_account(account)

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
                                deal_pos = 111
                                target_pos = 0
                                industry = positions.loc[code]['INDUSTRY']
                                QA_util_log_info('##JOB Now Start Selling {code} ==== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                                SELL(client, account, strategy_id, account_info, trading_date, code, name, industry, deal_pos, target_pos, target=None, close=0, type = 'end', test = test)

                        if code in list(target_tar.index):
                            if res2[2] == True:
                                ###买入信号
                                send_actionnotice(strategy_id,'{code}{name}:{trading_date}-{mark_tm}'.format(code=code,name=name,trading_date=trading_date,mark_tm=mark_tm),'买入信号',direction = 'BUY',offset=mark_tm,volume=None)
                                price = round(QA_fetch_get_stock_realtm_bid(code)+0.01,2)
                                deal_pos = round(50000 / price,2)
                                target_pos = deal_pos
                                industry = target_tar.loc[code]['INDUSTRY']
                                QA_util_log_info('##JOB Now Start Buying {code} ===== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                                BUY(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target=None, close=0, type = 'end', test = test)

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