
from .concat_predict import concat_predict,concat_predict_real,concat_predict_crawl,concat_predict_hedge,concat_predict_index
from .setting import working_dir, percent, exceptions, top
from QUANTTOOLS.Market.MarketTools import predict_base

def predict(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, 'stock_xg', 'prediction', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_real(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_real, 'stock_xg_real', 'prediction_real', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_crawl(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_crawl, 'stock_xg_real', 'prediction_real', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_hedge(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_hedge, 'hedge_xg', 'prediction_hedge', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_index(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_index, 'index_xg', 'prediction_index', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

