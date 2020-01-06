from datetime import datetime,timedelta
from QUANTTOOLS.QAStockTradingDay.train import train
from QUANTTOOLS.QAStockTradingDay.running import predict
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_if_trade,QA_util_get_last_day

def job111(date):
    if QA_util_if_trade(date):
        print(datetime.strptime(date, "%Y-%m-%d").weekday())
        if datetime.strptime(date, "%Y-%m-%d").weekday() == 4:
            train(date, working_dir=working_dir)
            predict(date)
        else:
            predict(date)
    elif datetime.strptime(date, "%Y-%m-%d").weekday() == 5:
        pass
    else:
        date = QA_util_get_last_day(date)
        predict(date)