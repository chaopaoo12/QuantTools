import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,QA_fetch_stock_alpha_adv,QA_fetch_stock_technical_index_adv)
from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_basic_info_tushare
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
import QUANTAXIS as QA

def rolling_n(data, N):
    data = data.rolling(window=N).agg([lambda x:x['PE'].rank(pct=True).iloc[-1],
                                        lambda x:(x['PE']/x['PE'].median()-1).iloc[-1],
                                        lambda x:x['PB'].rank(pct=True).iloc[-1],
                                        lambda x:(x['PB']/x['PB'].median()-1).iloc[-1],
                                        lambda x:x['PEG'].rank(pct=True).iloc[-1],
                                        lambda x:(x['PEG']/x['PEG'].median()-1).iloc[-1],
                                        lambda x:x['PS'].rank(pct=True).iloc[-1],
                                        lambda x:(x['PS']/x['PS'].median()-1).iloc[-1],
                                        lambda x:(x['PE_RANK']/x['PE_RANK'].median()-1).iloc[-1],
                                        lambda x:(x['PB_RANK']/x['PB_RANK'].median()-1).iloc[-1]]
                                       )
    return(data)

def perank(data):
    data = data.set_index('date')
    data[['PE_10PCT','PE_10VAL','PB_10PCT','PB_10VAL','PEG_10PCT','PEG_10VAL','PS_10PCT','PS_10VAL','PERANK_10VAL','PBRANK_10VAL']] = rolling_n(data, 10)
    data[['PE_20PCT','PE_20VAL','PB_20PCT','PB_20VAL','PEG_20PCT','PEG_20VAL','PS_20PCT','PS_20VAL','PERANK_20VAL','PBRANK_20VAL']] = rolling_n(data, 20)
    data[['PE_30PCT','PE_30VAL','PB_30PCT','PB_30VAL','PEG_30PCT','PEG_30VAL','PS_30PCT','PS_30VAL','PERANK_30VAL','PBRANK_30VAL']] = rolling_n(data, 30)
    data[['PE_60PCT','PE_60VAL','PB_60PCT','PB_60VAL','PEG_60PCT','PEG_60VAL','PS_60PCT','PS_60VAL','PERANK_60VAL','PBRANK_60VAL']] = rolling_n(data, 60)
    data[['PE_90PCT','PE_90VAL','PB_90PCT','PB_90VAL','PEG_90PCT','PEG_90VAL','PS_90PCT','PS_90VAL','PERANK_90VAL','PBRANK_90VAL']] = rolling_n(data, 90)
    return(data)

def QA_fetch_get_stock_financial_percent(code,start_date,end_date):
    start = QA_util_get_pre_trade_date(start_date,91)
    fianacial = QA_fetch_stock_fianacial_adv(code,start,end_date).data[['PB', 'PE', 'PEG', 'PS','PB_RANK','PE_RANK']]
    try:
        fianacial = fianacial.groupby('code').apply(perank).loc[pd.Series(pd.date_range(start_date, end_date, freq='D')).apply(lambda x: str(x)[0:10])].reset_index()
        fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['PB', 'PE', 'PEG', 'PS','PB_RANK','PE_RANK']]]
        fianacial['date_stamp'] = fianacial['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
        return(fianacial)
    except:
        print('No Data')

