from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_fianacial_adv
from QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info)
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_get_trade_range)
import numpy as np

def rolling_calc(data,N=5):
    data[['PB_M', 'PE_TTM_M', 'PEEGL_TTM_M', 'PEG_M', 'PS_M']] = data[['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS']].rolling(window=N).mean()
    data[['PB_2', 'PE_TTM_2', 'PEEGL_TTM_2', 'PEG_2', 'PS_2']] = data[['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS']].rolling(window=N).agg(lambda x:np.percentile(x, 75))
    data[['PB_7', 'PE_TTM_7', 'PEEGL_TTM_7', 'PEG_7', 'PS_7']] = data[['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS']].rolling(window=N).agg(lambda x:np.percentile(x, 25))
    data = data.assign(PB_VAL = data.PB / data.PB_M - 1,
                       PB_DN = data.PB / data.PB_2 - 1,
                       PB_UP = data.PB / data.PB_7 - 1,

                       PE_TTM_VAL = data.PE_TTM / data.PE_TTM_M - 1,
                       PE_TTM_DN = data.PE_TTM / data.PE_TTM_2 - 1,
                       PE_TTM_UP = data.PE_TTM / data.PE_TTM_7 - 1,

                       PEEGL_VAL = data.PEEGL_TTM / data.PEEGL_TTM_M - 1,
                       PEEGL_DN = data.PEEGL_TTM / data.PEEGL_TTM_2 - 1,
                       PEEGL_UP = data.PEEGL_TTM / data.PEEGL_TTM_7 - 1,

                       PEG_VAL = data.PEG / data.PEG_M - 1,
                       PEG_DN = data.PEG / data.PEG_2 - 1,
                       PEG_UP = data.PEG / data.PEG_7 - 1,

                       PS_VAL = data.PS / data.PS_M - 1,
                       PS_DN = data.PS / data.PS_2 - 1,
                       PS_UP = data.PS / data.PS_7 - 1,
                       )

    return(data[['PB_VAL','PB_DN','PB_UP'
        ,'PE_TTM_VAL','PE_TTM_DN','PE_TTM_UP'
        ,'PEEGL_VAL','PEEGL_DN','PEEGL_UP'
        ,'PEG_VAL','PEG_DN','PEG_UP'
        ,'PS_VAL','PS_DN','PS_UP']])

def perank(data):

    data[['PB_30VAL','PB_30DN','PB_30UP','PE_30VAL','PE_30DN','PE_30UP','PEEGL_30VAL','PEEGL_30DN','PEEGL_30UP','PEG_30VAL','PEG_30DN','PEG_30UP','PS30VAL','PS_30DN','PS_30UP']] = rolling_calc(data, 30)
    data[['PB_60VAL','PB_60DN','PB_60UP','PE_60VAL','PE_60DN','PE_60UP','PEEGL_60VAL','PEEGL_60DN','PEEGL_60UP','PEG_60VAL','PEG_60DN','PEG_60UP','PS60VAL','PS_60DN','PS_60UP']] = rolling_calc(data, 60)
    data[['PB_90VAL','PB_90DN','PB_90UP','PE_90VAL','PE_90DN','PE_90UP','PEEGL_90VAL','PEEGL_90DN','PEEGL_90UP','PEG_90VAL','PEG_90DN','PEG_90UP','PS90VAL','PS_90DN','PS_90UP']] = rolling_calc(data, 90)
    return(data[['PB_30VAL','PB_30DN','PB_30UP','PE_30VAL','PE_30DN','PE_30UP','PEEGL_30VAL','PEEGL_30DN','PEEGL_30UP','PEG_30VAL','PEG_30DN','PEG_30UP','PS30VAL','PS_30DN','PS_30UP',
                 'PB_60VAL','PB_60DN','PB_60UP','PE_60VAL','PE_60DN','PE_60UP','PEEGL_60VAL','PEEGL_60DN','PEEGL_60UP','PEG_60VAL','PEG_60DN','PEG_60UP','PS60VAL','PS_60DN','PS_60UP',
                 'PB_90VAL','PB_90DN','PB_90UP','PE_90VAL','PE_90DN','PE_90UP','PEEGL_90VAL','PEEGL_90DN','PEEGL_90UP','PEG_90VAL','PEG_90DN','PEG_90UP','PS90VAL','PS_90DN','PS_90UP'
                 ]])


def QA_fetch_get_stock_financial_percent(code,start_date,end_date):
    start = QA_util_get_pre_trade_date(start_date,91)
    fianacial = QA_fetch_stock_fianacial_adv(code,start,end_date).data[['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS','PB_RANK','PE_RANK']]
    try:
        fianacial = fianacial.groupby('code').apply(perank).loc[(QA_util_get_trade_range(start_date, end_date),slice(None)),].reset_index()
        fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['PB', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PS','PB_RANK','PE_RANK']]]
        fianacial['date_stamp'] = fianacial['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
        return(fianacial)
    except:
        QA_util_log_info('JOB No Data for {code} ====== from {_from} to {_to}'.format(code=code, _from=start_date, _to=end_date))