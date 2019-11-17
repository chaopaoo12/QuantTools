from datetime import datetime,timedelta
from QUANTTOOLS.QAStockTradingDay.train import train
from QUANTTOOLS.QAStockTradingDay.running import predict
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTTOOLS.QAStockTradingDay.trading import trading
import QUANTAXIS as QA

def job111(date):
    if datetime.strptime(date, "%Y-%m-%d").weekday() == 5:
        train(date, working_dir=working_dir)
    elif datetime.strptime(date, "%Y-%m-%d").weekday() < 5:
        predict(date)
    else:
        pass


def trading(date):
    if QA.QA_util_if_trade(date) == True:
        trading(date)
    else:
        pass