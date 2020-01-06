#from datetime import datetime,timedelta
#from QUANTTOOLS.QAStockTradingDay.train import train
#from QUANTTOOLS.QAStockTradingDay.running import predict
#from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_last_day


def job111(trading_date):
    print(QA_util_get_last_day(str(trading_date)))

#def job111(trading_date):
#    if QA_util_if_trade(trading_date):
#        if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 4:
#            train(trading_date, working_dir=working_dir)
#            predict(trading_date)
#        else:
#            predict(trading_date)
#    elif datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 5:
#        pass
#    else:
#        print(trading_date)
#        print(QA_util_get_last_day(trading_date))
#        predict(QA_util_get_last_day(trading_date))