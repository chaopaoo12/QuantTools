from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Ananlysis.Trends.trends import stock_daily, stock_hourly
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import QA_fetch_stock_name
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_hour, get_quant_data
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

                if mark_tm == "09:30:00":
                    stm = QA_util_get_pre_trade_date(trading_date) + ' ' + '15:00:00'
                elif mark_tm == '13:00:00':
                    stm = trading_date + ' ' + '11:30:00'
                else:
                    stm = trading_date + ' ' + mark_tm

                data = get_quant_data_hour(QA_util_get_pre_trade_date(trading_date),trading_date,list(set(positions.code.tolist()+list(target_tar.index))), type= 'real')
                res1 = data.loc[stm][['SKDJ_K_30M','SKDJ_TR_30M','SKDJ_K_HR','SKDJ_TR_HR','SKDJ_CROSS1_HR','CROSS_JC_HR','CROSS_SC_HR','SKDJ_CROSS2_HR','MA5_HR','MA5_30M','MA10_HR','MA60_HR','CCI_HR','CCI_CROSS1_HR','CCI_CROSS2_HR']].sort_values('SKDJ_K_HR')

                target_list = [i for i in list(res1.index) if i not in positions.code.tolist()]
                QA_util_log_info('##JOB Now Time ==== {}'.format(str(mark_tm)), ui_log = None)

                ####job1 小时级报告 指数小时级跟踪
                for code_list in [positions.code.tolist(), target_list]:
                    for code in code_list:

                        name = QA_fetch_stock_name(code)
                        QA_util_log_info('##JOB Now Code {stm} ==== {code}({name})'.format(stm=str(stm),code=str(code),name=str(name)), ui_log = None)
                        try:
                            res2 = res1.loc[code]
                            QA_util_log_info('{code}{name}-{stm}:hourly: {hourly}'.format(code=code,name=name,stm=stm,hourly=res2.SKDJ_TR_HR))
                        except:
                            res2 = None
                            QA_util_log_info('error')

                        try:
                            if res2[1] == True:
                                ###卖出信号1
                                QA_util_log_info('{code}{name}-{trading_date}:daily: {daily}; weekly: {weekly}'.format(code=code,name=name,trading_date=trading_date,daily=res1[0],weekly=res1[1]))
                                send_actionnotice(strategy_id,'{code}{name}:{trading_date}-{mark_tm}'.format(code=code,name=name,trading_date=trading_date,mark_tm=mark_tm),'卖出信号',direction = 'SELL',offset=mark_tm,volume=None)

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

    tm = datetime.datetime.now().strftime("%H:%M:%S")
    morning_begin = "09:30:00"
    morning_end = "11:30:00"
    afternoon_begin = "13:00:00"
    afternoon_end = "15:00:00"
    marktm_list = ['10:00:00',"10:30:00",'11:00:00',"11:30:00",'13:30:00',"14:00:00", "14:30:00", "15:00:00"]
    action_list = ["09:30:00",'10:00:00',"10:30:00",'11:00:00',"13:00:00",'13:30:00', "14:00:00", "14:30:00"]

    ##init+确定时间
    a = marktm_list + [tm]
    a.sort()

    if a.index(tm) > 0:
        mark_tm = a[a.index(tm)-1]
        action_tm = action_list[a.index(tm)]
    elif a.index(tm) == 0:
        mark_tm = '15:00:00'
        action_tm = '09:30:00'

    tm = int(time.strftime("%H%M%S",time.strptime(tm, "%H:%M:%S")))
    QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)

    while tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_end, "%H:%M:%S"))):

        QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
        client = get_Client()
        sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
        positions = positions[positions['可用余额'] > 0]
        account_info = client.get_account(account)

        if mark_tm == '15:00:00':
            stm = QA_util_get_pre_trade_date(trading_date) + ' ' + '15:00:00'
        else:
            stm = trading_date + ' ' + mark_tm

        ##分析数据
        print('tm',tm,'mark_tm',mark_tm,int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))))
        while tm <= int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):

            if tm > int(time.strftime("%H%M%S",time.strptime(mark_tm, "%H:%M:%S"))):
                QA_util_log_info('##JOB Now Build Trading Frame ==== {}'.format(str(trading_date)), ui_log = None)
                hold = float(positions[positions.code==code]['成本价'])
                price = float(QA_fetch_get_stock_realtm_bid(code))
                close = float(QA_fetch_get_stock_close(code))
                high = float(QA_fetch_get_stock_realtime(code).high)
                #data = get_quant_data_min(QA_util_get_pre_trade_date(trading_date),trading_date,positions.code.tolist(), type= 'real')
                #res1 = data.loc[stm][['SKDJ_K_30M','SKDJ_TR_30M','SKDJ_K_15M','SKDJ_TR_15M','SKDJ_CROSS1_30M','CROSS_JC_30M','CROSS_SC_30M','SKDJ_CROSS2_30M','MA5_30M','MA10_30M','MA60_30M','CCI_30M','CCI_CROSS1_30M','CCI_CROSS2_30M']].sort_values('SKDJ_K_HR')
            else:
                time.sleep(60)
                tm = int(datetime.datetime.now().strftime("%H%M%S"))

        ##开市前休息
        while tm < int(time.strftime("%H%M%S",time.strptime(morning_begin, "%H:%M:%S"))):
            time.sleep(60)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        ##午休
        while tm >= int(time.strftime("%H%M%S",time.strptime(morning_end, "%H:%M:%S"))) and tm <= int(time.strftime("%H%M%S",time.strptime(afternoon_begin, "%H:%M:%S"))):
            time.sleep(600)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        ##action
        while tm <= int(time.strftime("%H%M%S",time.strptime(action_tm, "%H:%M:%S"))) and action_tm is not None:

            if tm > int(time.strftime("%H%M%S",time.strptime(action_tm, "%H:%M:%S"))):
                ##action
                ####job1 小时级报告 指数小时级跟踪
                for code in positions.code.tolist():
                    name = QA_fetch_stock_name(code)
                    QA_util_log_info('##JOB Now Code {code}({name}) ==== 成本:{hold} 昨收:{close} 今高:{high} 现价:{price}'.format(code=str(code),name=str(name),hold=str(hold),high=str(high), close = str(close), price = str(price)), ui_log = None)
                    hold = price / hold - 1

                    if close > high:
                        warning_line = price /close - 1
                    else:
                        warning_line = price / high-1

                    if hold <= -0.05 :
                        msg = '跌破开仓位-5%:止损'
                    #elif warning_line <= -0.05 and hold > 0:
                    #    msg = '高点回撤-5%:止盈'
                    else:
                        msg = None
                        ###卖出信号1
                    if msg is not None:
                        send_actionnotice(strategy_id,'{code}{name}:{msg}'.format(code=code,name=name, msg=msg),'卖出信号',direction = 'SELL',offset=None,volume=None)
                        deal_pos = get_StockPos(code, client, account)
                        target_pos = 0
                        industry = str(positions[positions.code == code]['INDUSTRY'])
                        QA_util_log_info('##JOB Now Start Selling {code} ===='.format(code = code), ui_log = None)
                        SELL(client, account, strategy_id, account_info, trading_date, code, name, industry, deal_pos, target_pos, target=None, close=0, type = 'end', test = True)
                        time.sleep(1)
                    #except:
                    #        pass
            else:
                time.sleep(60)
                tm = int(datetime.datetime.now().strftime("%H%M%S"))

        ##update mark_tm action_tm
        if marktm_list.index(mark_tm) == len(marktm_list) - 1:
            mark_tm = marktm_list[0]
        else:
            mark_tm = marktm_list[marktm_list.index(mark_tm) + 1]

        if marktm_list.index(mark_tm) == len(marktm_list) -1:
            action_tm = None
        else:
            action_tm = action_list[action_list.index(action_tm) + 1]

        ###update time
        tm = int(datetime.datetime.now().strftime("%H%M%S"))

    ##收市
    if tm >= int(time.strftime("%H%M%S", time.strptime(afternoon_end, "%H:%M:%S"))):
        ###time out
        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)


if __name__ == '__main__':
    pass