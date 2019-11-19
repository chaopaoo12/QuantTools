from datetime import datetime,timedelta
from QUANTTOOLS.QAStockTradingDay.train import train
from QUANTTOOLS.QAStockTradingDay.running import predict
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day

def job111(date):
    if datetime.strptime(date, "%Y-%m-%d").weekday() == 5:
        train(date, working_dir=working_dir)
    elif datetime.strptime(date, "%Y-%m-%d").weekday() < 5:
        #date = QA_util_get_last_day(date)
        predict(date)
    else:
        pass
