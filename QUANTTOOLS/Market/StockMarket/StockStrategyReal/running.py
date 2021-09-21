#coding :utf-8

from .concat_predict import (concat_predict,concat_predict_hour,concat_predict_15min,concat_predict_hourmark,
                             concat_predict_real,concat_predict_crawl,concat_predict_hedge,concat_predict_neut,
                             concat_predict_index,concat_predict_indexhour,concat_predict_index15min)
from .setting import working_dir, percent, exceptions, top
from QUANTTOOLS.Market.MarketTools import predict_base, predict_index_base, predict_index_dev, predict_stock_dev,base_report, load_data
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_hour,get_index_quant_data,get_quant_data,get_quant_data_hour,get_quant_data_30min
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date,QA_util_get_real_date
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import (QA_util_get_trade_range)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_index_name
from QUANTTOOLS.Model.FactorTools.base_tools import find_stock

def predict(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_xg', file_name = 'prediction', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_real(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_real, model_name = 'stock_xg_real', file_name = 'prediction_real', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_crawl(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_crawl, model_name = 'stock_xg', file_name = 'prediction_crawl', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_hedge(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_hedge, model_name = 'hedge_xg', file_name = 'prediction_hedge', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_neut(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_neut, model_name = 'stock_xg_nn', file_name = 'prediction_stock_xg_nn', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

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

def predict_hourly_mark(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_hourmark, model_name = 'stock_mark_hour', file_name = 'prediction_stock_mark_hour', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)


def predict_minly(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_15min, model_name = 'stock_mars_min', file_name = 'prediction_stock_mars_min', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)


def predict_index_summary(trading_date, top_num=top, working_dir=working_dir):
    predict_index_dev(trading_date,
                      predict_func1 =concat_predict_index, predict_func2 =concat_predict_indexhour, predict_func3 =None,
                      day_moel = 'index_xg', hour_model='index_mars_hour', min_model=None,
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
    #r_tar, prediction_tar, prediction = load_data(concat_predict, trading_date, working_dir, 'stock_xg', 'prediction')
    #wk_list = data[data.SKDJ_K_WK <= 30][['SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
    data = data[(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 10)&(data.ROE >= 1)&(data.SHORT10.abs() < 0.01)&(data.SHORT20.abs() < 0.01)&(data.MA60.abs() < 0.01)][['INDUSTRY','NETPROFIT_INRATE','ROE_TTM','PE_TTM','SHORT10','SHORT20','MA60_C','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','TARGET20','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','MA60']]
    #pe_list = prediction_tar.loc[data.index]
    #target_pool1 = wk_list.reset_index().sort_values(by=['date','SKDJ_K'],ascending=[False,True]).set_index(['date','code'])
    base_report(trading_date, '观察报告', **{'初始清单':data})


def index_predict_watch(trading_date, working_dir=working_dir):
    trading_date = QA_util_get_real_date(trading_date)
    data = get_index_quant_data(QA_util_get_pre_trade_date(trading_date,90),trading_date,type='crawl', norm_type=None)
    r = data[['PASS_MARK']].groupby('code').describe()
    r.columns=['cnt','mean','std','min','p25','median','p75','max']
    rr = r.join(data.loc[trading_date][['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K_HR','SKDJ_TR_HR','RSI3','RSI2','RSI3_C','RSI2_C']])
    rr['per'] = rr['p75'] / abs(rr['p25'])
    rr1 = rr[((rr.per >= 1.5)|(rr['std'] >=1.8))&(rr.p75 >= 1)]

    res = data.loc[(QA_util_get_trade_range(QA_util_get_pre_trade_date(trading_date,5),trading_date),rr1.reset_index().code.tolist()),].reset_index()

    rr1 = res.assign(NAME=res.code.apply(lambda x:QA_fetch_index_name(x)))[['date','code','NAME','SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K_HR','SKDJ_TR_HR','RSI3','RSI2','RSI3_C','RSI2_C','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
    rr1 = rr1.sort_values(by=['date','SKDJ_K'],ascending=[False,True]).set_index(['date','code'])
    r_tar, prediction_tar, prediction = load_data(concat_predict, trading_date, working_dir, 'stock_xg', 'prediction')

    kk = prediction_tar.loc[(trading_date,find_stock(list(rr1[rr1.SKDJ_K <= 40].loc[trading_date].index))),].sort_values('SKDJ_K')

    base_report(trading_date, '市场观察报告', **{'主线趋势指数': rr1,
                                           '日线机会清单': rr1[(rr1.SKDJ_K <= 40)],
                                           '小时线机会清单': rr1[rr1.SKDJ_K_HR <= 30],
                                           '周线机会清单':rr1[rr1.SKDJ_K_WK <= 30],
                                           '待选股池清单':kk})

def predict_3(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_mars_day', file_name = 'prediction_stock_mars_day', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_3_norm(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_neut, model_name = 'stock_mars_nn', file_name = 'prediction_stock_mars_nn', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_norm(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict_neut, model_name = 'stock_xg_nn', file_name = 'prediction_stock_xg_nn', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_target(trading_date, working_dir=working_dir):
    r_tar, prediction_tar, prediction = load_data(concat_predict, trading_date, working_dir, model_name = 'stock_xg', file_name = 'prediction')


    #rrr = prediction_tar.loc[(slice(None),find_stock(res)),].reset_index().sort_values(by=['date','RANK'],ascending=[False,True]).set_index(['date','code'])

    data = get_quant_data(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='crawl', block=False, sub_block=False,norm_type=None)
    pe_list = data[(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 10)&(data.SHORT10.abs() < 0.01)&(data.SHORT20 > -0.01)&(data.SHORT20 < 0)&(data.MA60_C > 0)]
    pe_list = prediction_tar.loc[pe_list.index]

    r_tar = prediction_tar[(prediction_tar.O_PROB > 0.5)&(prediction_tar.TARGET5.isnull())].drop_duplicates(subset='NAME',keep='last').reset_index().set_index('code')

    target_list = list(set((list(r_tar.index) +
                            pe_list[(pe_list.y_pred==1)&(pe_list.TARGET5.isnull())].reset_index().code.tolist() +
                            find_stock(['880727','880730','880505','880560','880951','880491'])
                            )))
    target_list = [i for i in target_list if i.startswith('688') == False]
    target_pool = prediction_tar.loc[(slice(None),target_list),].loc[QA_util_get_real_date(trading_date)]

    #hour = get_quant_data_hour(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='crawl', block=False, sub_block=False,norm_type=None)
    #min30 = get_quant_data_30min(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='model', block=False, sub_block=False,norm_type=None)

    #res = min30.join(hour[[i for i in hour.columns if i not in min30.columns]]).groupby('code').fillna(method='ffill')
    #res = res[res.date.isin(QA_util_get_trade_range(QA_util_get_pre_trade_date(trading_date,5),trading_date))]

    #in_list = res[((res.SKDJ_CROSS2_HR == 1) | (res.CROSS_JC_HR == 1)) & (res.SKDJ_TR_30M > 0) & (res.MA5_30M > 0)].loc[(slice(None),target_list),][['date','SKDJ_K_HR','SKDJ_TR_HR','SKDJ_K_30M','SKDJ_TR_30M','MA5_30M','PASS_MARK','TARGET','TARGET3','TARGET5']]

    #out_ist = res[((res.SKDJ_CROSS1_30M == 1) | (res.SKDJ_TR_30M < 1)) & (res.MA5_30M < 0)].loc[(slice(None),target_list),][['date','SKDJ_K_HR','SKDJ_TR_HR','SKDJ_K_30M','SKDJ_TR_30M','MA5_30M','PASS_MARK','TARGET','TARGET3','TARGET5']]

    base_report(trading_date, '模型汇总报告', **{'本日选股': target_pool[(target_pool.RSI3 > target_pool.RSI2)&(target_pool.SKDJ_K < 40)&(target_pool.ATRR >= 0.03)].sort_values('SKDJ_K_HR').head(50),
                                           'PE选股': pe_list[(pe_list.y_pred == 1)&(pe_list.RSI3 > pe_list.RSI2)&(target_pool.ATRR >= 0.05)],
                                           #'进场信号':in_list,
                                           #'出场信号':out_ist
    })