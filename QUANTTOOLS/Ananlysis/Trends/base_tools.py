from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_btc_day,QA_fetch_get_btc_min,
                                           QA_fetch_get_gold_day,QA_fetch_get_gold_min,
                                           QA_fetch_get_money_day,QA_fetch_get_money_min,QA_fetch_get_diniw_min,
                                           QA_fetch_get_usstock_day_xq,QA_fetch_get_stock_indicator_realtime)
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator_short,get_indicator

def check(data):
    res = data.iloc[-1:].reset_index().set_index('code')
    return(res.SKDJ_TR)

def check_hour(data, date):
    res = data.loc[date].reset_index().set_index('code')
    return(res[['SKDJ_TR','SKDJ_CROSS1','SKDJ_CROSS2','MA5']])

def trends_money(MONEY, date):
    day = QA_fetch_get_money_day(MONEY,date)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_money = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_money = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_money = get_indicator_short(data_money,'day')
    data_money = data_money.assign(SKDJ_TR = (data_money.SKDJ_CROSS1*-1+ data_money.SKDJ_CROSS2*1)/(data_money.SKDJ_CROSS1+data_money.SKDJ_CROSS2),
                                 SHORT_TR = (data_money.SHORT20 > 0)*1,
                                 LONG_TR = (data_money.LONG60 > 0)*1,
                                 TERNS = ((data_money.SHORT20 > 0) * (data_money.LONG60 > 0) * (data_money.LONG_AMOUNT > 0) * 1)
                                 )
    data_money.SKDJ_TR = data_money.SKDJ_TR.fillna(method='ffill')
    week_money = get_indicator_short(week_money,'week')
    week_money = week_money.assign(SKDJ_TR = (week_money.SKDJ_CROSS1*-1+ week_money.SKDJ_CROSS2*1)/(week_money.SKDJ_CROSS1+week_money.SKDJ_CROSS2),
                                   SHORT_TR = (week_money.SHORT20 > 0)*1,
                                   LONG_TR = (week_money.LONG60 > 0)*1,
                                   TERNS = ((week_money.SHORT20 > 0) * (week_money.LONG60 > 0) * (week_money.LONG_AMOUNT > 0) * 1)
                                   )
    week_money.SKDJ_TR = week_money.SKDJ_TR.fillna(method='ffill')
    return(data_money, week_money)

def trends_btc(BTC):
    day = QA_fetch_get_btc_day(BTC)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_btc = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_btc = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_btc = get_indicator_short(data_btc,'day')
    data_btc = data_btc.assign(SKDJ_TR = (data_btc.SKDJ_CROSS1*-1+ data_btc.SKDJ_CROSS2*1)/(data_btc.SKDJ_CROSS1+data_btc.SKDJ_CROSS2),
                            SHORT_TR = (data_btc.SHORT20 > 0)*1,
                            LONG_TR = (data_btc.LONG60 > 0)*1,
                            TERNS = ((data_btc.SHORT20 > 0) * (data_btc.LONG60 > 0) * (data_btc.LONG_AMOUNT > 0) * 1)
                            )
    data_btc.SKDJ_TR = data_btc.SKDJ_TR.fillna(method='ffill')
    week_btc = get_indicator_short(week_btc,'week')
    week_btc = week_btc.assign(SKDJ_TR = (week_btc.SKDJ_CROSS1*-1+ week_btc.SKDJ_CROSS2*1)/(week_btc.SKDJ_CROSS1+week_btc.SKDJ_CROSS2),
                                 SHORT_TR = (week_btc.SHORT20 > 0)*1,
                                 LONG_TR = (week_btc.LONG60 > 0)*1,
                                 TERNS = ((week_btc.SHORT20 > 0) * (week_btc.LONG60 > 0) * (week_btc.LONG_AMOUNT > 0) * 1)
                                 )
    week_btc.SKDJ_TR = week_btc.SKDJ_TR.fillna(method='ffill')
    return(data_btc, week_btc)

def trends_gold(GOLD, date):
    day = QA_fetch_get_gold_day(GOLD,date)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_gold = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_gold = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_gold = get_indicator_short(data_gold,'day')
    data_gold = data_gold.assign(SKDJ_TR = (data_gold.SKDJ_CROSS1*-1+ data_gold.SKDJ_CROSS2*1)/(data_gold.SKDJ_CROSS1+data_gold.SKDJ_CROSS2),
                                   SHORT_TR = (data_gold.SHORT20 > 0)*1,
                                   LONG_TR = (data_gold.LONG60 > 0)*1,
                                   TERNS = ((data_gold.SHORT20 > 0) * (data_gold.LONG60 > 0) * (data_gold.LONG_AMOUNT > 0) * 1)
                                   )
    data_gold.SKDJ_TR = data_gold.SKDJ_TR.fillna(method='ffill')
    week_gold = get_indicator_short(week_gold,'week')
    week_gold = week_gold.assign(SKDJ_TR = (week_gold.SKDJ_CROSS1*-1+ week_gold.SKDJ_CROSS2*1)/(week_gold.SKDJ_CROSS1+week_gold.SKDJ_CROSS2),
                                 SHORT_TR = (week_gold.SHORT20 > 0)*1,
                                 LONG_TR = (week_gold.LONG60 > 0)*1,
                                 TERNS = ((week_gold.SHORT20 > 0) * (week_gold.LONG60 > 0) * (week_gold.LONG_AMOUNT > 0) * 1)
                                 )
    week_gold.SKDJ_TR = week_gold.SKDJ_TR.fillna(method='ffill')
    return(data_gold, week_gold)

def trends_stock(code, start_date, end_date, period='day', type='before'):
    day = QA_fetch_get_usstock_day_xq(code, start_date, end_date, period=period, type=type)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last','volume':'sum','amount':'sum'})
    data_index = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_index = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_index = get_indicator_short(data_index,'day')
    data_index = data_index.assign(SKDJ_TR = (data_index.SKDJ_CROSS1*-1+ data_index.SKDJ_CROSS2*1)/(data_index.SKDJ_CROSS1+data_index.SKDJ_CROSS2),
                               SHORT_TR = (data_index.SHORT20 > 0)*1,
                               LONG_TR = (data_index.LONG60 > 0)*1,
                               TERNS = ((data_index.SHORT20 > 0) * (data_index.LONG60 > 0) * (data_index.LONG_AMOUNT > 0) * 1)
                               )
    data_index.SKDJ_TR = data_index.SKDJ_TR.fillna(method='ffill')
    week_index = get_indicator_short(week_index,'week')
    week_index = week_index.assign(SKDJ_TR = (week_index.SKDJ_CROSS1*-1+ week_index.SKDJ_CROSS2*1)/(week_index.SKDJ_CROSS1+week_index.SKDJ_CROSS2),
                                   SHORT_TR = (week_index.SHORT20 > 0)*1,
                                   LONG_TR = (week_index.LONG60 > 0)*1,
                                   TERNS = ((week_index.SHORT20 > 0) * (week_index.LONG60 > 0) * (week_index.LONG_AMOUNT > 0) * 1)
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