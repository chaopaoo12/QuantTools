
from .running import predict,predict_real,predict_hedge, predict_crawl

def daily_run(trading_date):
    predict(trading_date)

def daily_run_real(trading_date):
    predict_real(trading_date)

def daily_run_crawl(trading_date):
    predict_crawl(trading_date)

def daily_run_hedge(trading_date):
    predict_hedge(trading_date)