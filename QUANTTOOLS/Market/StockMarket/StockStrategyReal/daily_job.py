
from .running import predict_index,predict,predict_3,predict_hourly,predict_index_summary,predict_stock_summary, \
    predict_target, predict_watch,index_predict_watch, predict_norm, predict_3_norm,predict_neut

def daily_run(trading_date):

    #TARGET 3
    predict_3(trading_date)
    #TARGET5 3
    predict_norm(trading_date)
    predict_watch(trading_date)
    #TARGET 3 neut
    predict_3_norm(trading_date)
    #TARGET5 3 neut
    predict_neut(trading_date)
    #predict_stock_summary(trading_date)

def index_run(trading_date):
    index_predict_watch(trading_date)
    predict_index_summary(trading_date)
    predict_target(trading_date)

def hourly_run(trading_date):
    predict_hourly(trading_date)