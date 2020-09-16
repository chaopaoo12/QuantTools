from datetime import datetime
from .train import train
from .running import predict
from .setting import working_dir
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date

def job111(trading_date):
    if QA_util_if_trade(trading_date):
        if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 4:
            predict(trading_date)
            train(trading_date, working_dir=working_dir)
            predict(trading_date)
        else:
            predict(trading_date)
    elif datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 5:
        pass
    else:
        predict(QA_util_get_real_date(trading_date))