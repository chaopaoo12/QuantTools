#coding :utf-8

from .concat_predict import (concat_predict,concat_predict_hour,concat_predict_15min,concat_predict_hourmark,
                             concat_predict_real,concat_predict_crawl,concat_predict_hedge,concat_predict_neut,
                             concat_predict_index,concat_predict_indexhour,concat_predict_index15min)
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import working_dir, percent, exceptions, top
from QUANTTOOLS.Market.MarketTools import predict_base, predict_index_base, predict_index_dev, predict_stock_dev,base_report, load_data
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data,get_quant_data
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date,QA_util_get_real_date
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import (QA_util_get_trade_range)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_index_name
from QUANTTOOLS.Model.FactorTools.base_tools import find_stock
from QUANTTOOLS.QAStockETL.QAUtil.QASQLBlockAnalystic import QA_Sql_BlockAnalystic,QA_Sql_BlockAnalysticS
from QUANTAXIS import QA_fetch_stock_block,QA_fetch_index_list_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_target,QA_fetch_index_target
import numpy as np
import pandas as pd

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

    codelist = data[(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 10)&(data.PE_TTM <= 10)&(data.PB > 0)&(data.ROE >= 0)].loc[QA_util_get_trade_range(QA_util_get_pre_trade_date(trading_date,1),trading_date)][['INDUSTRY','NETPROFIT_INRATE','ROE_TTM','PB','PE_TTM','ROE','GROSSMARGIN','SKDJ_K','SKDJ_K_WK']].sort_values('SKDJ_K_WK')
    res = data.loc[(trading_date,codelist.reset_index().code.unique().tolist()),][['INDUSTRY','NETPROFIT_INRATE','ROE_TTM','PE_TTM','SHORT10','SHORT20','LONG60','AVG5','MA60_C','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','TARGET20','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','MA60']]

    base_report(trading_date, '观察报告', **{'爆发清单':res[((res.SHORT10.abs() < 0.01)&(res.SHORT20.abs() < 0.02)&(res.LONG60.abs() < 0.05))&(res.AVG5 >= 5)],
                                         '下跌清单':res[((res.SHORT10.abs() < 0.01)&(res.SHORT20.abs() < 0.02)&(res.LONG60.abs() < 0.05))&(res.AVG5 <= -1)],
                                         'stay清单':res[((res.SHORT10.abs() < 0.01)&(res.SHORT20.abs() < 0.02)&(res.LONG60.abs() < 0.05))&(res.AVG5 < 5)&(res.AVG5 > -1)],
                                         '观察清单':codelist})


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
    #r_tar, prediction_tar, prediction = load_data(concat_predict, trading_date, working_dir, 'stock_xg', 'prediction')

    #kk = prediction_tar.loc[(trading_date,find_stock(list(rr1[rr1.SKDJ_K <= 40].loc[trading_date].index))),].sort_values('SKDJ_K')

    base_report(trading_date, '市场观察报告', **{'主线趋势指数': rr1,
                                           '日线机会清单': rr1[(rr1.SKDJ_K <= 40)],
                                           '小时线机会清单': rr1[rr1.SKDJ_K_HR <= 30],
                                           '周线机会清单':rr1[rr1.SKDJ_K_WK <= 30]})

def predict_3(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_mars_day', file_name = 'prediction_stock_mars_day', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

def predict_3_1(trading_date, top_num=top, working_dir=working_dir, exceptions=exceptions):
    predict_base(trading_date, concat_predict, model_name = 'stock_mars_day_1', file_name = 'prediction_stock_mars_day_1', top_num=top_num, percent=percent, working_dir=working_dir, exceptions=exceptions)

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

def filter_extreme_percentile(series, min = 0.10, max = 0.90):
    series = series.sort_values()
    q = series.quantile([min,max])
    return np.clip(series, q.iloc[0], q.iloc[1])

def f(x):
    return np.nanmean(filter_extreme_percentile(x))

def divv(x, y):
    if y == 0:
        return x - y
    else:
        return (x - y) / abs(y)

def block_func(trading_date):
    trading_date = QA_util_get_real_date(trading_date)
    #data = get_quant_data(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='crawl', block=False, sub_block=False,norm_type=None)
    data = QA_Sql_BlockAnalystic(trading_date,trading_date)
    index_info = QA_fetch_index_list_adv()
    data = data.set_index('INDEX_CODE').join(index_info.rename(columns={'name':'BLN'})['BLN']).reset_index().rename(
        columns={'BLOCKNAME':'BLN'})
    res = data.groupby(['index','BLN'])[['TOTAL_MARKET','GROSSMARGIN','TURNOVERRATIOOFTOTALASSETS','OPERATINGRINRATE','PB','PE_TTM','ROE_TTM']].agg(f).rename(
        columns={'TOTAL_MARKET':'I_TM','GROSSMARGIN':'I_GM','TURNOVERRATIOOFTOTALASSETS':'I_TURNR','OPERATINGRINRATE':'I_OPINR','ROE_TTM':'I_ROE','PE_TTM':'I_PE','PB':'I_PB'})
    data = data.set_index(['index','BLN']).join(res).reset_index()
    data = data.assign(GM_RATE=(data.GROSSMARGIN-data.I_GM)/data.I_GM,
                       TURN_RATE=(data.TURNOVERRATIOOFTOTALASSETS-data.I_TURNR)/data.I_TURNR,
                       TM_RATE=(data.TOTAL_MARKET-data.I_TM)/data.I_TM,
                       OPINR_RATE=(data.OPERATINGRINRATE-data.I_OPINR)/data.I_OPINR,
                       PB_RATE=(data.PB-data.I_PB)/data.I_PB,
                       PE_RATE=(data.PE_TTM-data.I_PE)/data.I_PE,
                       ROE_RATE=(data.ROE_TTM-data.I_ROE)/data.I_ROE
                       )
    res = res.reset_index()
    res = res[~res.BLN.isin(['珠三角','次新股'])]
    ROE_line = np.nanpercentile(res.I_ROE,80)
    OPINR_line = np.nanpercentile(res.I_OPINR,80)
    data = data[data.CODE.isin([i for i in data.CODE.unique().tolist() if i.startswith('688') == False])]
    area1 = data[data.BLN.isin(res[(res.I_ROE >= ROE_line)&(res.I_OPINR >= OPINR_line)].BLN)]
    area2 = data[data.BLN.isin(res[(res.I_ROE >= ROE_line)&(res.I_OPINR < OPINR_line)].BLN)]
    res_a = res[(res.I_ROE >= ROE_line)&(res.I_OPINR >= OPINR_line)]
    res_b = area1[(area1.GROSSMARGIN > area1.I_GM)&(area1.OPERATINGRINRATE > area1.I_OPINR)]
    res_c = res[(res.I_ROE >= ROE_line)&(res.I_OPINR < OPINR_line)]
    res_d = area2[(area2.GROSSMARGIN > area2.I_GM)&(area2.OPERATINGRINRATE > area2.I_OPINR)]
    return(res_a,res_b,res_c,res_d)

def watch_func(start_date, end_date, working_dir=working_dir):

    res_a =[]
    res_b =[]
    res_c =[]
    res_d =[]
    for i in QA_util_get_trade_range(start_date, end_date):
        a,b,c,d=block_func(i)
        res_a.append(a.assign(date=i))
        res_b.append(b.assign(date=i))
        res_c.append(c.assign(date=i))
        res_d.append(d.assign(date=i))
    res_a = pd.concat(res_a).rename(columns={'index':'code'}).set_index(['date','code'])
    res_b = pd.concat(res_b).rename(columns={'CODE':'code'}).set_index(['date','code'])
    res_c = pd.concat(res_c).rename(columns={'index':'code'}).set_index(['date','code'])
    res_d = pd.concat(res_d).rename(columns={'CODE':'code'}).set_index(['date','code'])


    res_b['BLN'] = res_b.groupby(['date','code'])['BLN'].transform(lambda x: ','.join(x))
    rrr = res_b.reset_index().drop_duplicates(subset=['date','code']).set_index(['date','code'])

    res_d['BLN'] = res_d.groupby(['date','code'])['BLN'].transform(lambda x: ','.join(x))
    rrr1 = res_d.reset_index().drop_duplicates(subset=['date','code']).set_index(['date','code'])
    return(res_a, rrr, res_c, rrr1)

def block_func1(trading_date):
    trading_date = QA_util_get_real_date(trading_date)
    #data = get_quant_data(QA_util_get_pre_trade_date(trading_date,5),trading_date,type='crawl', block=False, sub_block=False,norm_type=None)
    data = QA_Sql_BlockAnalysticS(trading_date,trading_date).rename(
        columns={'BLOCKNAME':'BLN'})
    res = data.groupby(['BLN'])[['TOTAL_MARKET','GROSSMARGIN','TURNOVERRATIOOFTOTALASSETS','OPERATINGRINRATE','PB','PE_TTM','ROE_TTM']].agg(f).rename(
        columns={'TOTAL_MARKET':'I_TM','GROSSMARGIN':'I_GM','TURNOVERRATIOOFTOTALASSETS':'I_TURNR','OPERATINGRINRATE':'I_OPINR','ROE_TTM':'I_ROE','PE_TTM':'I_PE','PB':'I_PB'})
    data = data.set_index(['BLN']).join(res).reset_index()
    data = data.assign(GM_RATE=(data.GROSSMARGIN-data.I_GM)/data.I_GM,
                       TURN_RATE=(data.TURNOVERRATIOOFTOTALASSETS-data.I_TURNR)/data.I_TURNR,
                       TM_RATE=(data.TOTAL_MARKET-data.I_TM)/data.I_TM,
                       OPINR_RATE=(data.OPERATINGRINRATE-data.I_OPINR)/data.I_OPINR,
                       PB_RATE=(data.PB-data.I_PB)/data.I_PB,
                       PE_RATE=(data.PE_TTM-data.I_PE)/data.I_PE,
                       ROE_RATE=(data.ROE_TTM-data.I_ROE)/data.I_ROE
                       )
    res = res.reset_index()
    res = res[~res.BLN.isin(['珠三角','次新股'])]
    ROE_line = np.nanpercentile(res.I_ROE,80)
    OPINR_line = np.nanpercentile(res.I_OPINR,80)
    data = data[data.CODE.isin([i for i in data.CODE.unique().tolist() if i.startswith('688') == False])]
    area1 = data[data.BLN.isin(res[(res.I_ROE >= ROE_line)&(res.I_OPINR >= OPINR_line)].BLN)].sort_index()
    area2 = data[data.BLN.isin(res[(res.I_ROE >= ROE_line)&(res.I_OPINR < OPINR_line)].BLN)].sort_index()
    return(res[(res.I_ROE >= ROE_line)&(res.I_OPINR >= OPINR_line)],
           area1[((area1.GROSSMARGIN > area1.I_GM)&(area1.OPERATINGRINRATE > area1.I_OPINR))],
           res[(res.I_ROE >= ROE_line)&(res.I_OPINR < OPINR_line)],
           area2[((area2.GROSSMARGIN > area2.I_GM)&(area2.OPERATINGRINRATE > area2.I_OPINR))])

def watch_func1(start_date, end_date, working_dir=working_dir):

    res_a =[]
    res_b =[]
    res_c =[]
    res_d =[]
    for i in QA_util_get_trade_range(start_date, end_date):
        a,b,c,d=block_func1(i)
        res_a.append(a.assign(date=i))
        res_b.append(b.assign(date=i))
        res_c.append(c.assign(date=i))
        res_d.append(d.assign(date=i))
    res_a = pd.concat(res_a).rename(columns={'BLN':'code'}).set_index(['date','code'])
    res_b = pd.concat(res_b).rename(columns={'CODE':'code'}).set_index(['date','code'])
    res_c = pd.concat(res_c).rename(columns={'BLN':'code'}).set_index(['date','code'])
    res_d = pd.concat(res_d).rename(columns={'CODE':'code'}).set_index(['date','code'])

    stock_target = get_quant_data(start_date, end_date, type='crawl', block=False, sub_block=False,norm_type=None)[['RRNG','MA60_C','MA60_D','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','SKDJ_K_WK','SKDJ_TR_WK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]

    res_b['BLN'] = res_b.groupby(['date','code'])['BLN'].transform(lambda x: ','.join(x))
    rrr = res_b.reset_index().drop_duplicates(subset=['date','code']).set_index(['date','code'])\
        .join(stock_target)

    res_d['BLN'] = res_d.groupby(['date','code'])['BLN'].transform(lambda x: ','.join(x))
    rrr1 = res_d.reset_index().drop_duplicates(subset=['date','code']).set_index(['date','code'])\
        .join(stock_target)

    return(res_a, rrr, res_c, rrr1)


def block_watch(trading_date):
    start_date = QA_util_get_pre_trade_date(trading_date,5)
    end_date = trading_date
    res_a, res_b, res_c, res_d = watch_func(start_date, end_date)

    stock_target = get_quant_data(start_date, end_date,list(set(res_b.reset_index().code.tolist() + res_d.reset_index().code.tolist())), type='crawl', block=False, sub_block=False,norm_type=None)[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
    index_target = get_index_quant_data(start_date, end_date, list(set(res_a.reset_index().code.tolist() + res_c.reset_index().code.tolist())), type='crawl', norm_type=None)[['RRNG','MA60_C','MA60_D','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','SKDJ_K_WK','SKDJ_TR_WK','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]

    res_a = res_a.join(index_target)
    res_b = res_b.join(stock_target)
    res_c = res_c.join(index_target)
    res_d = res_d.join(stock_target)

    base_report(trading_date, '板块报告 一', **{'优质板块':res_a,
                                           '高潜板块':res_c})

    r_tar, xg, prediction = load_data(concat_predict, trading_date, working_dir, 'block_day', 'block_prediction')

    res_b = res_b.join(xg[['O_PROB','RANK']].rename(columns={'O_PROB':'block_xg'}))
    res_b['block_RANK'] = res_b['block_xg'].groupby('date').rank(ascending=False)

    res_d = res_d.join(xg[['O_PROB','RANK']].rename(columns={'O_PROB':'block_xg'}))
    res_d['block_RANK'] = res_d['block_xg'].groupby('date').rank(ascending=False)

    base_report(trading_date, '优质板块选股 二', **{
                                             '均线清单':res_b[res_b.RRNG.abs() <= 0.05],
                                             '轮动清单':res_b[res_b.block_RANK <= 5],
                                             '股池清单':res_b
                                             })
    base_report(trading_date, '潜力板块选股 二', **{
                                             '均线清单':res_d[res_d.RRNG.abs() <= 0.05],
                                             '轮动清单':res_d[res_d.block_RANK <= 5],
                                             '股池清单':res_d
                                             })
    base_report(trading_date, '综合选股报告 一', **{'综合选股':res_b[(res_b.RRNG.abs() <= 0.05)&(res_b.PB <= res_b.I_PB * 0.8)&(res_b.PE_TTM <= res_b.I_PE * 0.8)&(res_b.PE_TTM > 0)&(res_b.TM_RATE < -0.5)]
                                             })


def summary_func(trading_date):
    start_date = QA_util_get_pre_trade_date(trading_date,14)
    end_date = trading_date
    r_tar, xg, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg', 'prediction')
    r_tar, xg_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
    r_tar, mars_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
    r_tar, mars_day, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

    stock_target = get_quant_data(start_date, end_date, type='crawl', block=False, sub_block=False,norm_type=None)[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
    stock_res = stock_target[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','TAR','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK']]
    cols_name = ['RRNG','RRNG_HR','SHORT10','SHORT20','LONG60','TAR','AVG5','MA60_C','PASS_MARK', 'TARGET', 'TARGET3', 'TARGET4', 'TARGET5','TARGET10', 'y_pred', 'model', 'RANK']
    xg = stock_res.join(xg).assign(model='xg')
    xg_nn = stock_res.join(xg_nn).assign(model='xg_nn')
    mars_nn = stock_res.join(mars_nn).assign(model='mars_nn')
    mars_day = stock_res.join(mars_day).assign(model='mars_day')

    res = pd.concat([mars_day[(mars_day.y_pred==1)&(mars_day.RRNG.abs() < 0.1)][cols_name],
                     mars_nn[(mars_nn.y_pred==1)&(mars_nn.RRNG.abs() < 0.1)][cols_name],
                     xg_nn[(xg_nn.y_pred==1)&(xg_nn.RRNG.abs() < 0.1)][cols_name],
                     xg[(xg.y_pred==1)&(xg.RRNG.abs() < 0.1)][cols_name]])

    res['model'] = res.groupby(['date','code'])['model'].transform(lambda x: ','.join(x))
    res = res.reset_index().drop_duplicates(subset=['date','code']).set_index(['date','code']).sort_index()

    return(res,xg,xg_nn,mars_nn,mars_day)

def summary_watch(trading_date):
    res,xg,xg_nn,mars_nn,mars_day = summary_func(trading_date)
    try:
        rrr = res.loc[trading_date]
    except:
        rrr = None

    base_report(trading_date, '目标股池', **{'SUMMARY':res,
                                         'TARGET':rrr,
                                        'MARKS_DAY':mars_day[(mars_day.y_pred==1)&(mars_day.RRNG.abs() < 0.1)],
                                         'MARKS_NN':mars_nn[(mars_nn.y_pred==1)&(mars_nn.RRNG.abs() < 0.1)],
                                         'XG':xg[(xg.y_pred==1)&(xg.RRNG.abs() < 0.1)],
                                         'XG_NN':xg_nn[(xg_nn.y_pred==1)&(xg_nn.RRNG.abs() < 0.1)]
                                         })