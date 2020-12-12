
from .train import daymodel_train, train_hedge, train_index, minmodel_train, hourmodel_train
from .running import predict, predict_real, predict_hedge, predict_index, predict_daily, predict_hourly, predict_minly,predict_index_summary

from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date

from datetime import datetime

def train_base(trading_date, train_func, pred_func):
    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 5:

        if QA_util_if_trade(trading_date):
            pass
        else:
            trading_date = QA_util_get_real_date(trading_date)

        train_func(trading_date)
        pred_func(trading_date)
    else:
        pass

def daily_train(trading_date):
    train_base(trading_date, daymodel_train, predict_daily)

def hourly_train(trading_date):
    train_base(trading_date, hourmodel_train, predict_hourly)

def minly_train(trading_date):
    train_base(trading_date, minmodel_train, predict_minly)

def hedge_train(trading_date):
    train_base(trading_date, train_hedge, predict_hedge)

def index_train(trading_date):
    train_base(trading_date, train_index, predict_index_summary)
