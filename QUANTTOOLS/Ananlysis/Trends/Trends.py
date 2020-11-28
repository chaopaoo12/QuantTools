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
    for i in BTC:
        s = QA_fetch_get_btc_day(i)
        data_btc.append(s)
    data = QA_DataStruct_Stock_day(data_btc.set_index(['date','code']))
    indicator_day = get_indicator_short(data)

if len(GOLD)>0:
    data_gold = pd.DataFrame()
    for i in GOLD:
        s = QA_fetch_get_gold_day(i,date)
        data_gold.append(s)
    data = QA_DataStruct_Stock_day(data_gold.set_index(['date','code']))
    indicator_day = get_indicator_short(data)

if len(MONEY)>0:
    data_m = pd.DataFrame()
    for i in MONEY:
        s = QA_fetch_get_money_day(i,date)
        data_m.append(s)
    data = QA_DataStruct_Stock_day(data_m.set_index(['date','code']))
    indicator_day = get_indicator_short(data)

if len(INDEX)>0:
    data_index = pd.DataFrame()
    for i in INDEX:
        s = QA_fetch_get_usstock_day_xq(i,date)
        data_index.append(s)
    data = QA_DataStruct_Stock_day(data_index.set_index(['date','code']))
    indicator_day = get_indicator_short(data)