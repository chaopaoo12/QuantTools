from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_day_adv)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_usstock_xq_day_adv,QA_fetch_stock_half_adv,QA_fetch_stock_real
from QUANTAXIS.QAUtil import (QA_util_today_str,QA_util_date_stamp)
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_get_pre_trade_date,QA_util_get_trade_range
from QUANTTOOLS.QAStockETL.QAUtil.base_func import uspct
import numpy as np
import pandas as pd

def QA_fetch_get_stock_etlday(codes, start=None, end=None):
    if start is None:
        start = '2008-01-01'

    if end is None:
        end = QA_util_today_str()

    if start != end:
        rng = QA_util_get_trade_range(start, end)
    else:
        rng = str(start)[0:10]

    start_date = QA_util_get_pre_trade_date(start,100)
    data = QA_fetch_stock_day_adv(codes,start_date,end)
    try:
        res1 = data.to_qfq().data
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data.join(res1).fillna(0).reset_index()
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100 * data['adj_qfq']
        res = data.groupby('code').apply(uspct)
        res = res.set_index(['date','code']).loc[(rng,),].replace([np.inf, -np.inf], 0)
        res = res.where((pd.notnull(res)), None).reset_index()[['date','code','open','high','low','close','volume','amount',
                                                                'open_qfq','high_qfq','low_qfq','close_qfq','AVG_TOTAL_MARKET',
                                                                'LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW','LAG_AMOUNT',
                                                                'LAG2_MARKET','AVG_LAG2_MARKET',
                                                                'LAG3_MARKET','AVG_LAG3_MARKET',
                                                                'LAG5_MARKET','AVG_LAG5_MARKET',
                                                                'LAG10_MARKET','AVG_LAG10_MARKET',
                                                                'LAG20_MARKET','AVG_LAG20_MARKET',
                                                                'LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW',
                                                                'LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW',
                                                                'LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW',
                                                                'AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10',
                                                                'AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20',
                                                                'AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30',
                                                                'AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60',
                                                                'AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90',
                                                                'AVG5_T_MARKET','AVG5_A_MARKET','HIGH_5','LOW_5',
                                                                'AVG5_C_MARKET','AVG10_C_MARKET',
                                                                'AVG20_C_MARKET','AVG30_C_MARKET','AVG60_C_MARKET','AVG90_C_MARKET',
                                                                'RNG_L','RNG_5','RNG_10','RNG_20','RNG_30','RNG_60','RNG_90',
                                                                'AMT_L','AMT_5','AMT_10','AMT_20','AMT_30','AMT_60','AMT_90',
                                                                'MAMT_5','MAMT_10','MAMT_20','MAMT_30','MAMT_60','MAMT_90']]
    except:
        res=None
    return(res)

def QA_fetch_get_usstock_etlday(codes, start=None, end=None):
    if start is None:
        start = '2016-06-01'

    if end is None:
        end = QA_util_today_str()

    if start != end:
        rng = QA_util_get_trade_range(start, end)
    else:
        rng = str(start)[0:10]

    start_date = QA_util_get_pre_trade_date(start,100, 'us')
    data = QA_fetch_usstock_xq_day_adv(codes,start_date,end, 'us')
    try:
        res1 = data.to_qfq().data[['open','high','low','close','volume','amount','adj','adjust']]
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data[['open','high','low','close','volume','amount']].join(res1).fillna(0).reset_index()
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume'] * data['adj_qfq'] + data['adjust_qfq']
        res = data.groupby('code').apply(uspct)
        res = res.set_index(['date','code']).loc[(rng,),].replace([np.inf, -np.inf], 0)
        res = res.where((pd.notnull(res)), None).reset_index()[['date','code','open','high','low','close','volume','amount',
                                                                'open_qfq','high_qfq','low_qfq','close_qfq','AVG_TOTAL_MARKET',
                                                                'LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW','LAG_AMOUNT',
                                                                'LAG2_MARKET','AVG_LAG2_MARKET',
                                                                'LAG3_MARKET','AVG_LAG3_MARKET',
                                                                'LAG5_MARKET','AVG_LAG5_MARKET',
                                                                'LAG10_MARKET','AVG_LAG10_MARKET',
                                                                'LAG20_MARKET','AVG_LAG20_MARKET',
                                                                'LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW',
                                                                'LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW',
                                                                'LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW',
                                                                'AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10',
                                                                'AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20',
                                                                'AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30',
                                                                'AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60',
                                                                'AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90',
                                                                'AVG5_T_MARKET','AVG5_A_MARKET','HIGH_5','LOW_5',
                                                                'AVG5_C_MARKET','AVG10_C_MARKET',
                                                                'AVG20_C_MARKET','AVG30_C_MARKET','AVG60_C_MARKET','AVG90_C_MARKET',
                                                                'RNG_L','RNG_5','RNG_10','RNG_20','RNG_30','RNG_60','RNG_90',
                                                                'AMT_L','AMT_5','AMT_10','AMT_20','AMT_30','AMT_60','AMT_90',
                                                                'MAMT_5','MAMT_10','MAMT_20','MAMT_30','MAMT_60','MAMT_90']]
    except:
        res=None
    return(res)

def QA_fetch_get_stock_etlhalf(codes, start=None, end=None):
    if start is None:
        start = '2008-01-01'

    if end is None:
        end = QA_util_today_str()

    if start != end:
        rng = QA_util_get_trade_range(start, end)
    else:
        rng = str(start)[0:10]

    start_date = QA_util_get_pre_trade_date(start,100)
    data = QA_fetch_stock_half_adv(codes,start_date,end)
    try:
        res1 = data.to_qfq().data
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data.join(res1).fillna(0).reset_index()
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100 * data['adj_qfq']
        res = data.groupby('code').apply(uspct)
        res = res.set_index(['date','code']).loc[(rng,),].replace([np.inf, -np.inf], 0)
        res = res.where((pd.notnull(res)), None).reset_index()[['date','code','open','high','low','close','volume','amount',
                                                                'open_qfq','high_qfq','low_qfq','close_qfq','AVG_TOTAL_MARKET',
                                                                'LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW','LAG_AMOUNT',
                                                                'LAG2_MARKET','AVG_LAG2_MARKET',
                                                                'LAG3_MARKET','AVG_LAG3_MARKET',
                                                                'LAG5_MARKET','AVG_LAG5_MARKET',
                                                                'LAG10_MARKET','AVG_LAG10_MARKET',
                                                                'LAG20_MARKET','AVG_LAG20_MARKET',
                                                                'LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW',
                                                                'LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW',
                                                                'LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW',
                                                                'AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10',
                                                                'AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20',
                                                                'AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30',
                                                                'AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60',
                                                                'AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90',
                                                                'AVG5_T_MARKET','AVG5_A_MARKET','HIGH_5','LOW_5',
                                                                'AVG5_C_MARKET','AVG10_C_MARKET',
                                                                'AVG20_C_MARKET','AVG30_C_MARKET','AVG60_C_MARKET','AVG90_C_MARKET',
                                                                'RNG_L','RNG_5','RNG_10','RNG_20','RNG_30','RNG_60','RNG_90',
                                                                'AMT_L','AMT_5','AMT_10','AMT_20','AMT_30','AMT_60','AMT_90',
                                                                'MAMT_5','MAMT_10','MAMT_20','MAMT_30','MAMT_60','MAMT_90']]
    except:
        res=None
    return(res)

def QA_fetch_get_stock_etlreal(codes, start=None, end=None):
    if start is None:
        start = QA_util_today_str()

    if end is None:
        end = QA_util_today_str()

    if start != end:
        rng = QA_util_get_trade_range(start, end)
    else:
        rng = str(start)[0:10]

    start_date = QA_util_get_pre_trade_date(start,100)
    end_date = QA_util_get_pre_trade_date(end, 1)
    data = QA_fetch_stock_half_adv(codes,start_date,end_date)
    try:
        res1 = data.to_qfq().data
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data.join(res1).fillna(0).reset_index()
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100 * data['adj_qfq']

        real = QA_fetch_stock_real(codes,end,end).drop(columns=['date_stamp','avg_price','prev_close'])
        real = real.assign(open_qfq=real.open,
                           high_qfq=real.high,
                           low_qfq=real.low,
                           close_qfq=real.close,
                           volume_qfq=real.volume,
                           amount_qfq=real.amount)
        real['AVG_TOTAL_MARKET'] =  real['amount']/real['volume']/100
        data = data.append(real).set_index(['date','code']).sort_index()

        res = data.groupby('code').apply(uspct)
        res = res.loc[(rng,),].replace([np.inf, -np.inf], 0)
        res = res.where((pd.notnull(res)), None).reset_index()[['date','code','open','high','low','close','volume','amount',
                                                                'open_qfq','high_qfq','low_qfq','close_qfq','AVG_TOTAL_MARKET',
                                                                'LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW','LAG_AMOUNT',
                                                                'LAG2_MARKET','AVG_LAG2_MARKET',
                                                                'LAG3_MARKET','AVG_LAG3_MARKET',
                                                                'LAG5_MARKET','AVG_LAG5_MARKET',
                                                                'LAG10_MARKET','AVG_LAG10_MARKET',
                                                                'LAG20_MARKET','AVG_LAG20_MARKET',
                                                                'LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW',
                                                                'LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW',
                                                                'LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW',
                                                                'AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10',
                                                                'AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20',
                                                                'AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30',
                                                                'AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60',
                                                                'AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90',
                                                                'AVG5_T_MARKET','AVG5_A_MARKET','HIGH_5','LOW_5',
                                                                'AVG5_C_MARKET','AVG10_C_MARKET',
                                                                'AVG20_C_MARKET','AVG30_C_MARKET','AVG60_C_MARKET','AVG90_C_MARKET',
                                                                'RNG_L','RNG_5','RNG_10','RNG_20','RNG_30','RNG_60','RNG_90',
                                                                'AMT_L','AMT_5','AMT_10','AMT_20','AMT_30','AMT_60','AMT_90',
                                                                'MAMT_5','MAMT_10','MAMT_20','MAMT_30','MAMT_60','MAMT_90']]
        res = res.assign(date_stamp=res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    except:
        res=None
    return(res)