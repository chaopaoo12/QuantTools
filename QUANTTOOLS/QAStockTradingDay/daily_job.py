from datetime import datetime,timedelta
from QUANTTOOLS.QAStockTradingDay.train import train
from QUANTTOOLS.QAStockTradingDay.running import predict
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password

def job111(date):
    if datetime.strptime(date, "%Y-%m-%d").weekday() == 5:
        train(date, working_dir=working_dir)
    elif datetime.strptime(date, "%Y-%m-%d").weekday() < 5:
        predict(date, account1='name:client-1', working_dir=working_dir)
    else:
        pass
