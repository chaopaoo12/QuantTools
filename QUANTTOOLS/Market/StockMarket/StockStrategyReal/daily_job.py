
from .running import predict_index,predict_daily,predict_hourly,predict_index_summary,predict_stock_summary

def daily_run(trading_date):
    predict_stock_summary(trading_date)

def index_run(trading_date):
    predict_index_summary(trading_date)

def hourly_run(trading_date):
    predict_hourly(trading_date)