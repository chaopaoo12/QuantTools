from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_btc_day,QA_fetch_get_btc_min,
                                           QA_fetch_get_gold_day,QA_fetch_get_gold_min,
                                           QA_fetch_get_money_day,QA_fetch_get_money_min,QA_fetch_get_diniw_min,
                                           QA_fetch_get_usstock_day_xq,QA_fetch_get_stock_indicator_realtime)
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator_short,get_indicator
import datetime

def check(data):
    res = data.iloc[-1:].reset_index().set_index('code')
    return(res[['SKDJ_TR','SKDJ_CROSS1','SKDJ_CROSS2']])

def check_hour(data, date):
    res = data.loc[date].reset_index().set_index('code')
    return(res[['SKDJ_TR','SKDJ_CROSS1','SKDJ_CROSS2']])

def trends_money(MONEY, date):
    day = QA_fetch_get_money_day(MONEY,date)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_money = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_money = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_money = get_indicator_short(data_money,'day')
    week_money = get_indicator_short(week_money,'week')
    return(data_money, week_money)

def trends_btc(BTC):
    day = QA_fetch_get_btc_day(BTC)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_btc = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_btc = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_btc = get_indicator_short(data_btc,'day')
    week_btc = get_indicator_short(week_btc,'week')
    return(data_btc, week_btc)

def trends_gold(GOLD, date):
    day = QA_fetch_get_gold_day(GOLD,date)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
    data_gold = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_gold = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_gold = get_indicator_short(data_gold,'day')
    week_gold = get_indicator_short(week_gold,'week')
    return(data_gold, week_gold)

def trends_stock(code, start_date, end_date, period='day', type='before'):
    day = QA_fetch_get_usstock_day_xq(code, start_date, end_date, period=period, type=type)
    week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last','volume':'sum','amount':'sum'})
    data_index = QA_DataStruct_Stock_day(day.drop('date_stamp',axis=1).set_index(['date','code']))
    week_index = QA_DataStruct_Stock_day(week.reset_index().set_index(['date','code']))
    data_index = get_indicator(data_index,'day')
    week_index = get_indicator(week_index,'week')
    return(data_index, week_index)

def trends_stock_hour(code, start_date, end_date, type='hour'):
    hour = QA_fetch_get_stock_indicator_realtime(code, start_date, end_date, type=type)
    return(hour)