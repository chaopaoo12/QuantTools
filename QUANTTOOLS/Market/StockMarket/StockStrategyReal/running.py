#coding :utf-8

from .concat_predict import (concat_predict,concat_predict_hour,concat_predict_15min,
                             concat_predict_real,concat_predict_crawl,concat_predict_hedge,
                             concat_predict_index,concat_predict_indexhour,concat_predict_index15min)
from .setting import working_dir, percent, exceptions, top
from QUANTTOOLS.Market.MarketTools import predict_base, predict_index_base, predict_index_dev, predict_stock_dev,base_report
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_hour,get_index_quant_data,get_quant_data
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date,QA_util_get_real_date


def predict(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_xg', file_name = 'prediction', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_real(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_real, model_name = 'stock_xg_real', file_name = 'prediction_real', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_crawl(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_crawl, model_name = 'stock_xg_real', file_name = 'prediction_crawl', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_hedge(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_hedge, model_name = 'hedge_xg', file_name = 'prediction_hedge', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)


def predict_index(trading_date, working_dir=working_dir):
    predict_index_base(trading_date, concat_predict_index, model_name = 'index_mars_day', file_name = 'prediction_index_mars_day', top_num=10, working_dir=working_dir)

def predict_indexhour(trading_date, working_dir=working_dir):
    predict_index_base(trading_date, concat_predict_indexhour, model_name = 'index_mars_hour', file_name = 'prediction_index_mars_hour', top_num=10, working_dir=working_dir)

def predict_index15min(trading_date, working_dir=working_dir):
    predict_index_base(trading_date, concat_predict_index15min, model_name = 'index_mars_min', file_name = 'prediction_index_mars_min', top_num=10, working_dir=working_dir)


def predict_daily(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_mars_day', file_name = 'prediction_stock_mars_day', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_hourly(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_hour, model_name = 'stock_mars_hour', file_name = 'prediction_stock_mars_hour', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_minly(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_15min, model_name = 'stock_mars_min', file_name = 'prediction_stock_mars_min', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)


def predict_index_summary(trading_date, top_num=top, working_dir=working_dir):
    predict_index_dev(trading_date,
                      predict_func1 =concat_predict_index, predict_func2 =concat_predict_indexhour, predict_func3 =None,
                      day_moel = 'index_mars_day', hour_model='index_mars_hour', min_model=None,
                      file_name = 'prediction_index_summary', top_num=top_num, working_dir=working_dir)

def predict_stock_summary(trading_date, top_num=top, working_dir=working_dir):
    predict_stock_dev(trading_date,
                      xg_predict_func = concat_predict,predict_func1 =concat_predict, predict_func2 =None, predict_func3 =None,
                      xg_model = 'stock_xg', day_moel = 'stock_mars_day', hour_model=None, min_model=None,
                      file_name = 'prediction_stock_summary',
                      top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_watch(trading_date, working_dir=working_dir):
    trading_date = QA_util_get_real_date(trading_date)
    data = get_quant_data(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='crawl', block=False, sub_block=False,norm_type=None)
    wk_list = data[(data.SKDJ_CROSS2_WK == 1)&(data.CCI_WK > 0)].index
    pe_list = data[(data.ROE_RATE > 1)&(data.PE_RATE < 1)&(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 15)&(data.PE_TTM <= 30)].index
    #both_list = [i for i in wk_list if i in pe_list]
    target_pool,prediction,start,end,Model_Date = concat_predict(trading_date, working_dir, type = 'crawl', model_name = 'stock_mars_day')
    target_pool1 = target_pool.reindex(index=wk_list).dropna(how='all')
    target_pool2 = target_pool.reindex(index=pe_list).dropna(how='all')
    target_pool3 = target_pool.reindex(index=pe_list).reindex(index=wk_list).dropna(how='all')
    #target_pool1,prediction,start,end,Model_Date = concat_predict(trading_date, working_dir, code = wk_list, type = 'crawl', model_name = 'stock_mars_day')
    #target_pool2,prediction,start,end,Model_Date = concat_predict(trading_date, working_dir, code = pe_list, type = 'crawl', model_name = 'stock_mars_day')
    base_report(trading_date, '观察报告', **{'低估值清单': target_pool2, '周线趋势清单': target_pool1, '复合清单': target_pool3})
