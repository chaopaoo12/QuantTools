from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_btc_day,QA_fetch_get_btc_min,
                                           QA_fetch_get_gold_day,QA_fetch_get_gold_min,
                                           QA_fetch_get_money_day,QA_fetch_get_money_min,QA_fetch_get_diniw_min,
                                           QA_fetch_get_usstock_day_xq,QA_fetch_get_stock_indicator_realtime,
                                           QA_fetch_get_globalindex_day, QA_fetch_get_innerfuture_day)
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator_short,get_indicator
import numpy as np
from scipy import stats


def per25(x):
    return(np.percentile(x, 25))

def per75(x):
    return(np.percentile(x, 75))

def perc(x):
    x = list(x)
    tar = x[-1]
    return(stats.percentileofscore(x, tar))

def check(data):
    res = data.iloc[-1:].reset_index().set_index('code')
    return(res.SKDJ_TR)

def check_hour(data, date):
    res = data.loc[date].reset_index().set_index('code')
    return(res[['SKDJ_TR','SKDJ_CROSS1','SKDJ_CROSS2','MA5']])

def indicator(data, type):
    data_ind = get_indicator_short(data, type)
    data_ind = data_ind.assign(SKDJ_TR=(data_ind.SKDJ_CROSS1*-1+ data_ind.SKDJ_CROSS2*1)/(data_ind.SKDJ_CROSS1+data_ind.SKDJ_CROSS2),
                           SHORT_TR=(data_ind.SHORT20 > 0)*1,
                           LONG_TR=(data_ind.LONG60 > 0)*1,
                           TERNS=((data_ind.SHORT20 > 0) * (data_ind.LONG60 > 0) * (data_ind.LONG_AMOUNT > 0) * 1)
                           )
    data_ind.SKDJ_TR = data_ind.SKDJ_TR.fillna(method='ffill')
    return(data_ind)

def trends_func(func, code, date):
    day = func(code, date)
    data_index = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    data_ind = indicator(data_index, 'day')
    data_ind[['mean','per25','per75','perc']] = data_ind['close'].rolling(1800,min_periods=50).agg(['mean', per25, per75, perc])

    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    week_index = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    week_ind = indicator(week_index, 'week')
    return(data_ind, week_ind)

def trends_func1(func, code):
    day = func(code)
    data_index = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    data_ind = indicator(data_index, 'day')
    data_ind[['mean','per25','per75','perc']] = data_ind['close'].rolling(1800,min_periods=50).agg(['mean', per25, per75, perc])

    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    week_index = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    week_ind = indicator(week_index, 'week')
    return(data_ind, week_ind)

def trends_money(MONEY, date):
    data, week = trends_func(QA_fetch_get_money_day, MONEY, date)
    return(data, week)

def trends_btc(BTC):
    data, week = trends_func1(QA_fetch_get_btc_day, BTC)
    return(data, week)

def trends_gold(GOLD, date):
    data, week = trends_func(QA_fetch_get_gold_day, GOLD, date)
    return(data, week)

def trends_globalindex(GOLD, date):
    data, week = trends_func(QA_fetch_get_globalindex_day, GOLD)
    return(data, week)

def trends_future(GOLD, date):
    data, week = trends_func(QA_fetch_get_innerfuture_day, GOLD, date)
    return(data, week)

def trends_stock(code, start_date, end_date, period='day', type='before'):
    day = QA_fetch_get_usstock_day_xq(code, start_date, end_date, period=period, type=type)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last','volume':'sum','amount':'sum'})
    data_index = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_index = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_index = get_indicator_short(data_index,'day')
    data_index[['mean','per25','per75','perc']] = data_index['close'].rolling(1800).agg(['mean', per25, per75, perc])
    data_index = data_index.assign(SKDJ_TR=(data_index.SKDJ_CROSS1*-1+ data_index.SKDJ_CROSS2*1)/(data_index.SKDJ_CROSS1+data_index.SKDJ_CROSS2),
                                   SHORT_TR=(data_index.SHORT20 > 0)*1,
                                   LONG_TR=(data_index.LONG60 > 0)*1,
                                   TERNS=((data_index.SHORT20 > 0) * (data_index.LONG60 > 0) * (data_index.LONG_AMOUNT > 0) * 1)
                                   )
    data_index.SKDJ_TR = data_index.SKDJ_TR.fillna(method='ffill')
    week_index = get_indicator_short(week_index,'week')
    week_index = week_index.assign(SKDJ_TR=(week_index.SKDJ_CROSS1*-1+ week_index.SKDJ_CROSS2*1)/(week_index.SKDJ_CROSS1+week_index.SKDJ_CROSS2),
                                   SHORT_TR=(week_index.SHORT20 > 0)*1,
                                   LONG_TR=(week_index.LONG60 > 0)*1,
                                   TERNS=((week_index.SHORT20 > 0) * (week_index.LONG60 > 0) * (week_index.LONG_AMOUNT > 0) * 1)
                                   )
    week_index.SKDJ_TR = week_index.SKDJ_TR.fillna(method='ffill')
    return(data_index, week_index)

def trends_stock_hour(code, start_date, end_date, type='hour'):
    hour = QA_fetch_get_stock_indicator_realtime(code, start_date, end_date, type=type)
    return(hour)

def trends_btc_hour(BTC):
    day = QA_fetch_get_btc_min(BTC, type=15)
    data_btc = day.set_index(['datetime','code']).rename(columns={'vol':'volume'}).assign(amount=0)
    data_btc = QA_DataStruct_Stock_min(data_btc)
    data_btc = get_indicator_short(data_btc,'min')
    data_btc = data_btc.assign(SKDJ_TR = (data_btc.SKDJ_CROSS1*-1+ data_btc.SKDJ_CROSS2*1)/(data_btc.SKDJ_CROSS1+data_btc.SKDJ_CROSS2),
                       SHORT_TR = (data_btc.SHORT20 > 0)*1,
                       LONG_TR = (data_btc.LONG60 > 0)*1,
                       TERNS = ((data_btc.SHORT20 > 0) * (data_btc.LONG60 > 0) * (data_btc.LONG_AMOUNT > 0) * 1)
                       )
    data_btc.SKDJ_TR = data_btc.SKDJ_TR.fillna(method='ffill')
    return(data_btc)