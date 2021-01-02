from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_fianacial_adv
from QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info)
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_get_trade_range)


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
    data[['PE_VAL','PEEGL_VAL','PB_VAL','PEG_VAL','PS_VAL']] = \
        res1.agg({'PE_TTM':rolling_median,
                  'PEEGL_TTM':rolling_median,
                  'PB':rolling_median,
                  'PEG':rolling_median,
                  'PS':rolling_median}).reset_index(level=0,drop=True)
    return(data[['PE_VAL','PEEGL_VAL','PB_VAL','PEG_VAL','PS_VAL']])

def rolling_calc1(data,N):
    res1 = data.groupby('code').rolling(window=N)
    data[['PE_VAL','PE_DN','PE_UP',
          'PEEGL_VAL','PEEGL_DN','PEEGL_UP',
          'PB_VAL','PB_DN','PB_UP',
          'PEG_VAL','PEG_DN','PEG_UP',
          'PS_VAL','PS_DN','PS_UP']] = res1.agg({'PE_TTM':[ rolling_median,rolling_down,rolling_up],
                                                  'PEEGL_TTM':[ rolling_median,rolling_down,rolling_up],
                                                  'PB':[ rolling_median,rolling_down,rolling_up],
                                                  'PEG':[ rolling_median,rolling_down,rolling_up],
                                                  'PS':[ rolling_median,rolling_down,rolling_up]}).reset_index(level=0,drop=True)
    return(data[['PE_VAL','PE_DN','PE_UP',
                 'PEEGL_VAL','PEEGL_DN','PEEGL_UP',
                 'PB_VAL','PB_DN','PB_UP',
                 'PEG_VAL','PEG_DN','PEG_UP',
                 'PS_VAL','PS_DN','PS_UP']])

def perank(data):
    data[['PE_10VAL','PEEGL_10VAL','PB_10VAL','PEG_10VAL','PS_10VAL']] = rolling_calc(data, 10)
    data[['PE_20VAL','PEEGL_20VAL','PB_20VAL','PEG_20VAL','PS_20VAL']] = rolling_calc(data, 20)
    data[['PE_30VAL','PE_30DN','PE_30UP','PEEGL_30VAL','PEEGL_30DN','PEEGL_30UP','PB_30VAL','PB_30DN','PB_30UP','PEG_30VAL','PEG_30DN','PEG_30UP','PS_30VAL','PS_30DN','PS_30UP']] = rolling_calc1(data, 30)
    data[['PE_60VAL','PE_60DN','PE_60UP','PEEGL_60VAL','PEEGL_60DN','PEEGL_60UP','PB_60VAL','PB_60DN','PB_60UP','PEG_60VAL','PEG_60DN','PEG_60UP','PS_60VAL','PS_60DN','PS_60UP']] = rolling_calc1(data, 60)
    data[['PE_90VAL','PE_90DN','PE_90UP','PEEGL_90VAL','PEEGL_90DN','PEEGL_90UP','PB_90VAL','PB_90DN','PB_90UP','PEG_90VAL','PEG_90DN','PEG_90UP','PS_90VAL','PS_90DN','PS_90UP']] = rolling_calc1(data, 90)
    return(data[['PE_10VAL','PEEGL_10VAL','PB_10VAL','PEG_10VAL','PS_10VAL',
                 'PE_20VAL','PEEGL_20VAL','PB_20VAL','PEG_20VAL','PS_20VAL',
                 'PE_30VAL','PE_30DN','PE_30UP','PEEGL_30VAL','PEEGL_30DN','PEEGL_30UP','PB_30VAL','PB_30DN','PB_30UP','PEG_30VAL','PEG_30DN','PEG_30UP','PS_30VAL','PS_30DN','PS_30UP',
                 'PE_60VAL','PE_60DN','PE_60UP','PEEGL_60VAL','PEEGL_60DN','PEEGL_60UP','PB_60VAL','PB_60DN','PB_60UP','PEG_60VAL','PEG_60DN','PEG_60UP','PS_60VAL','PS_60DN','PS_60UP',
                 'PE_90VAL','PE_90DN','PE_90UP','PEEGL_90VAL','PEEGL_90DN','PEEGL_90UP','PB_90VAL','PB_90DN','PB_90UP','PEG_90VAL','PEG_90DN','PEG_90UP','PS_90VAL','PS_90DN','PS_90UP']])

def QA_fetch_get_stock_financial_percent(code,start_date,end_date):
    start = QA_util_get_pre_trade_date(start_date,91)
    fianacial = QA_fetch_stock_fianacial_adv(code,start,end_date).data[['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS','PB_RANK','PE_RANK']]
    print(fianacial)
    try:
        fianacial = fianacial.groupby('code').apply(perank).loc[(QA_util_get_trade_range(start_date, end_date),slice(None)),].reset_index()
        fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS','PB_RANK','PE_RANK']]]
        fianacial['date_stamp'] = fianacial['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
        return(fianacial)
    except:
        QA_util_log_info('JOB No Data for {code} ====== from {_from} to {_to}'.format(code=code, _from=start_date, _to=end_date))