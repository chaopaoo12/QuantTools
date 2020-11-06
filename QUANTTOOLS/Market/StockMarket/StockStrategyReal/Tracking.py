
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from QUANTTOOLS.Market.MarketTools import tracking_morning, tracking_afternon, tracking_base

def tracker1(trading_date, func = concat_predict, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account = 'name:client-1', working_dir = working_dir, exceptions = exceptions):
    start_time = '09:25:00'
    end_time = '15:00:00'
    tracking_base(trading_date, func, model_name, file_name, start_time, end_time, percent, account, working_dir, exceptions)

def tracker2(trading_date, func = concat_predict_real, model_name = 'stock_xg_real', file_name = 'prediction_real', percent = percent, account = 'name:client-1', working_dir = working_dir, exceptions = exceptions):
    start_time = '13:00:00'
    end_time = '15:00:00'
    tracking_base(trading_date, func, model_name, file_name, start_time, end_time, percent, account, working_dir, exceptions)

def Tracking(trading_date):
    tracker1(trading_date)
    #tracker2(trading_date)


if __name__ == '__main__':
    pass