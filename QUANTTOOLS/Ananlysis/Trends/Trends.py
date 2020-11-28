from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_btc_day,QA_fetch_get_btc_min,
                                           QA_fetch_get_gold_day,QA_fetch_get_gold_min,
                                           QA_fetch_get_money_day,QA_fetch_get_money_min,QA_fetch_get_diniw_min,
                                           QA_fetch_get_usstock_day_xq)
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator_short
from .setting import BTC, GOLD, MONEY, INDEX, FUTURE
from QUANTAXIS.QAUtil import (QA_util_log_info,QA_util_today_str)
import pandas as pd

date= QA_util_today_str()

if len(BTC)>0:
    data_btc = pd.DataFrame()
    week_btc = pd.DataFrame()
    for i in BTC:
        day = QA_fetch_get_btc_day(i)
        week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
        data_btc = data_btc.append(day)
        week_btc = week_btc.append(week)
    data_btc = QA_DataStruct_Stock_day(data_btc.drop('date_stamp',axis=1).set_index(['date','code']))
    week_btc = QA_DataStruct_Stock_day(week_btc.reset_index().set_index(['date','code']))
    data_btc = get_indicator_short(data_btc,'day')
    week_btc = get_indicator_short(week_btc,'week')

if len(GOLD)>0:
    data_gold = pd.DataFrame()
    week_gold = pd.DataFrame()
    for i in GOLD:
        day = QA_fetch_get_gold_day(i,date)
        week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
        data_gold = data_gold.append(day)
        week_gold = week_gold.append(week)
    data_gold = QA_DataStruct_Stock_day(data_gold.drop('date_stamp',axis=1).set_index(['date','code']))
    week_gold = QA_DataStruct_Stock_day(week_gold.reset_index().set_index(['date','code']))
    data_gold = get_indicator_short(data_gold,'day')
    week_gold = get_indicator_short(week_gold,'week')

if len(MONEY)>0:
    data_money = pd.DataFrame()
    week_money = pd.DataFrame()
    for i in MONEY:
        day = QA_fetch_get_money_day(i,date)
        week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
        data_money = data_money.append(day)
        week_money = week_money.append(week)
    data_money = QA_DataStruct_Stock_day(data_money.drop('date_stamp',axis=1).set_index(['date','code']))
    week_money = QA_DataStruct_Stock_day(week_money.reset_index().set_index(['date','code']))
    data_money = get_indicator_short(data_money,'day')
    week_money = get_indicator_short(week_money,'week')

if len(INDEX)>0:
    data_money = pd.DataFrame()
    week_money = pd.DataFrame()
    for i in INDEX:
        day = QA_fetch_get_usstock_day_xq(i,date,date)
        week = day.drop('date_stamp',axis=1).set_index(['date']).resample('W').agg({'code':'last','open':'first','high':'max','low':'min','close':'last'})
        data_money = data_money.append(day)
        week_money = week_money.append(week)
    data_money = QA_DataStruct_Stock_day(data_money.drop('date_stamp',axis=1).set_index(['date','code']))
    week_money = QA_DataStruct_Stock_day(week_money.reset_index().set_index(['date','code']))
    data_money = get_indicator_short(data_money,'day')
    week_money = get_indicator_short(week_money,'week')