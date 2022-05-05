from QUANTTOOLS.Market.StockMarket.StockStrategyReal import trading_new, tracking_new
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice

if __name__ == '__main__':

    if QA_util_if_trade(QA_util_today_str()):
        try:
            trading_new(QA_util_today_str())
        except:
            send_actionnotice('自动交易错误报告',
                              '错误报告:{}'.format(QA_util_today_str()),
                              '交易程序执行中断,请检查',
                              direction = 'HOLD',
                              offset='HOLD',
                              volume=None
                              )