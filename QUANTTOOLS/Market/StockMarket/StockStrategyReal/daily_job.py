
from .running import predict_index,predict,predict_3,predict_hourly,predict_index_summary,predict_stock_summary, predict_watch,index_predict_watch

def daily_run(trading_date):
    predict(trading_date)
    predict_3(trading_date)
    predict_watch(trading_date)
    #predict_stock_summary(trading_date)

def index_run(trading_date):
    index_predict_watch(trading_date)
    predict_index_summary(trading_date)

def hourly_run(trading_date):
    predict_hourly(trading_date)