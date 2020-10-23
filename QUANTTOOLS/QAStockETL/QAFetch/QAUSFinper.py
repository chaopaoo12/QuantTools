import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_usstock_xq_day_adv
from QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_log_info)
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_get_pre_trade_date,QA_util_get_trade_range)


def rolling_rank(data):
    return(data.rank(pct=True).iloc[-1])

def rolling_median(data):
    return((data/data.median()-1).iloc[-1])

def rolling_down(data):
    return((data/data.quantile(0.25)-1).iloc[-1])

def rolling_up(data):
    return((data/data.quantile(0.75)-1).iloc[-1])

def rolling_calc(data,N):
    res1 = data.groupby('code').rolling(window=N)
    data[['PE_PCT','PE_VAL',
          'PB_PCT','PB_VAL',
          'PS_PCT','PS_VAL',
          'PCF_PCT','PCF_VAL']] = \
        res1.agg({'PE_TTM':[rolling_rank, rolling_median],
                  'PEEGL_TTM':[rolling_rank, rolling_median],
                  'PB':[rolling_rank, rolling_median],
                  'PEG':[rolling_rank, rolling_median],
                  'PS':[rolling_rank, rolling_median]}).reset_index(level=0,drop=True)
    return(data[['PE_PCT','PE_VAL',
                 'PB_PCT','PB_VAL',
                 'PS_PCT','PS_VAL',
                 'PCF_PCT','PCF_VAL']])

def rolling_calc1(data,N):
    res1 = data.groupby('code').rolling(window=N)
    data[['PE_VAL','PE_DN','PE_UP','PE_PCT',
          'PB_VAL','PB_DN','PB_UP','PB_PCT',
          'PS_VAL','PS_DN','PS_UP','PS_PCT',
          'PCF_VAL','PCF_DN','PCF_UP','PCF_PCT'
          ]] = res1.agg({'PE':[ rolling_median,rolling_down,rolling_up,rolling_rank],
                          'PB':[ rolling_median,rolling_down,rolling_up,rolling_rank],
                          'PS':[ rolling_median,rolling_down,rolling_up, rolling_rank],
                          'PCF':[ rolling_median,rolling_down,rolling_up, rolling_rank]}).reset_index(level=0,drop=True)
    return(data[['PE_PCT','PE_VAL','PE_DN','PE_UP',
                 'PB_PCT','PB_VAL','PB_DN','PB_UP',
                 'PS_PCT','PS_VAL','PS_DN','PS_UP',
                 'PCF_PCT','PCF_VAL','PCF_DN','PCF_UP']])

def perank(data):
    #data = data.set_index('date')
    data[['PE_10PCT','PE_10VAL','PB_10PCT','PB_10VAL','PS_10PCT','PS_10VAL','PCF_10PCT','PCF_10VAL']] = rolling_calc(data, 10)
    data[['PE_20PCT','PE_20VAL','PB_20PCT','PB_20VAL','PS_20PCT','PS_20VAL','PCF_20PCT','PCF_20VAL']] = rolling_calc(data, 20)
    data[['PE_30PCT','PE_30VAL','PE_30DN','PE_30UP','PB_30PCT','PB_30VAL','PB_30DN','PB_30UP','PS_30PCT','PS_30VAL','PS_30DN','PS_30UP','PCF_30PCT','PCF_30VAL','PCF_30DN','PCF_30UP']] = rolling_calc1(data, 30)
    data[['PE_60PCT','PE_60VAL','PE_60DN','PE_60UP','PB_60PCT','PB_60VAL','PB_60DN','PB_60UP','PS_60PCT','PS_60VAL','PS_60DN','PS_60UP','PCF_60PCT','PCF_60VAL','PCF_60DN','PCF_60UP']] = rolling_calc1(data, 60)
    data[['PE_90PCT','PE_90VAL','PE_90DN','PE_90UP','PB_90PCT','PB_90VAL','PB_90DN','PB_90UP','PS_90PCT','PS_90VAL','PS_90DN','PS_90UP','PCF_90PCT','PCF_90VAL','PCF_90DN','PCF_90UP']] = rolling_calc1(data, 90)
    return(data[['PE_10PCT','PE_10VAL','PB_10PCT','PB_10VAL','PS_10PCT','PS_10VAL','PCF_10PCT','PCF_10VAL',
                 'PE_20PCT','PE_20VAL','PB_20PCT','PB_20VAL','PS_20PCT','PS_20VAL','PCF_20PCT','PCF_20VAL',
                 'PE_30PCT','PE_30VAL','PE_30DN','PE_30UP','PB_30PCT','PB_30VAL','PB_30DN','PB_30UP','PS_30PCT','PS_30VAL','PS_30DN','PS_30UP','PCF_30PCT','PCF_30VAL','PCF_30DN','PCF_30UP',
                 'PE_60PCT','PE_60VAL','PE_60DN','PE_60UP','PB_60PCT','PB_60VAL','PB_60DN','PB_60UP','PS_60PCT','PS_60VAL','PS_60DN','PS_60UP','PCF_60PCT','PCF_60VAL','PCF_60DN','PCF_60UP',
                 'PE_90PCT','PE_90VAL','PE_90DN','PE_90UP','PB_90PCT','PB_90VAL','PB_90DN','PB_90UP','PS_90PCT','PS_90VAL','PS_90DN','PS_90UP','PCF_90PCT','PCF_90VAL','PCF_90DN','PCF_90UP']])

def QA_fetch_get_usstock_financial_percent(code,start_date,end_date):
    start = QA_util_get_pre_trade_date(start_date,91, 'us')
    fianacial = QA_fetch_usstock_xq_day_adv(code,start,end_date).data[['pe', 'pb', 'ps', 'pcf']]
    fianacial.columns = [i.upper() for i in list(fianacial.columns)]
    try:
        fianacial = fianacial.groupby('code').apply(perank).loc[QA_util_get_trade_range(start_date, end_date, 'us')].reset_index()
        fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['PB', 'PE', 'PCF', 'PS']]]
        fianacial['date_stamp'] = fianacial['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
        return(fianacial)
    except:
        QA_util_log_info('JOB No Data for {code} ====== from {_from} to {_to}'.format(code=code, _from=start_date, _to=end_date))