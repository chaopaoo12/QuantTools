
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from QUANTTOOLS.Market.MarketTools import tracking_base

def Tracking(trading_date):
    tracking_base(trading_date, strategy_id = '机器学习1号',
                  func=concat_predict, model_name='stock_xg', file_name='prediction',
                  percent=percent, account='name:client-1', working_dir=working_dir, exceptions=exceptions)


if __name__ == '__main__':
    pass