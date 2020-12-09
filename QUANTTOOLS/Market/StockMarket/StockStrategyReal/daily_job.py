
from .running import predict,predict_real,predict_hedge, predict_crawl,predict_index,predict_daily,predict_hourly

def daily_run(trading_date):
    predict_daily(trading_date)
    predict_index(trading_date)

def hourly_run(trading_date):
    predict_hourly(trading_date)