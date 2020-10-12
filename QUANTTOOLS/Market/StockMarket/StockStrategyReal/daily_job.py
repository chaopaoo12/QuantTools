
from .running import predict
from .running_real import predict as predict_real


def daily_run(trading_date):

    predict(trading_date)


def daily_run_real(trading_date):

    predict_real(trading_date)
