from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq, QA_fetch_get_stock_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_min_adv
from scipy import stats
import pandas as pd
import numpy as np
import math
from QUANTAXIS.QAIndicator.base import CROSS

def first(rows):
    return rows[0]

def last(rows):
    return rows[-1]

def percentile(n):
    def percentile_(x):
        return np.nanpercentile(x, n/100)
    percentile_.__name__ = 'perc_%s' % n
    return percentile_

def rolling_ols(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(round(math.degrees(math.atan(model.slope)),4))

def rolling_slope(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(round(model.slope,4))

def rolling_atan(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(round(math.atan(model.slope),4))

def spc(data, N= 240):
    data[['VAMPC_DEGRESS','VAMPC_SLOPE','VAMPC_ATAN']]= data.rolling(window=N,min_periods=5).agg({'VAMP':[rolling_ols,rolling_slope,rolling_atan]})
    return(data)

#def spc5(data, N= 5):
#    data[['VAMP_C5',
#          'close_c5']]= data.rolling(window=N,min_periods=5).agg({'VAMP':rolling_ols,
#                                                                'close':rolling_ols})
#    return(data)

def spc5(data, N= 5):
    data[['VAMP_DEGRESS','VAMP_SLOPE','VAMP_ATAN',
          'CLOSE_DEGRESS','CLOSE_SLOPE','CLOSE_ATAN']]= data.rolling(window=N,min_periods=5).agg({'VAMP':[rolling_ols,rolling_slope,rolling_atan],
                                                                                                  'close':[rolling_ols,rolling_slope,rolling_atan]})
    return(data)

def sohlc(data, N= 240):
    data[['day_open','day_close','day_high','day_low']] = data.rolling(window=N, min_periods=1).agg({'open':first,'close':last,'high':'max','low':'min'})
    return(data)

def QA_fetch_get_stock_vwap(code, start_date, end_date, period = '1', type = 'crawl'):
    QA_util_log_info("JOB Get {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))

    if type == 'crawl':
        data = QA_fetch_stock_min_adv(code, start_date, end_date, frequence='1min').data
    elif type == 'real':
        data = QA_fetch_get_stock_min_sina(code=code, period=period, type='qfq')

    if data is not None and type == 'real':
        data = data.reset_index(drop=True).set_index(['datetime', 'code']).drop(columns=['date_stamp'])

    try:
        data = data.assign(date=data.reset_index().datetime.apply(lambda x:str(x)[0:10]).tolist(),
                           HM=data.reset_index().datetime.dt.strftime('%H:%M').values,
                           )
        data = data.assign(camt=data.groupby(['date','code'])['amount'].cumsum(),
                           cvolume=data.groupby(['date','code'])['volume'].cumsum())
        data[['open_p','close_p','high_p','low_p','AMT_P','VOL_P']] = \
            data.groupby(['date','code'])[['open','close','high','low','camt','cvolume']].shift()
        data[['open_p2','close_p2','high_p2','low_p2']] = \
            data.groupby(['date','code'])[['open','close','high','low']].shift(2)
        data[['AMT_P','VOL_P']] = data.groupby(['HM','code'])[['camt','cvolume']].shift()
        data['AMT_UP'] = data['camt'] / data['AMT_P'] - 1
        data['VAMP'] = data['camt'] / data['cvolume'] / 100
        data['DISTANCE'] = data['close'] / data['VAMP'] - 1
        data[['day_open', 'day_close', 'day_high', 'day_low']] = data.groupby(['date','code']).apply(lambda x: sohlc(x))[['day_open', 'day_close', 'day_high', 'day_low']]
        data['open_pct'] = data['close'] / data['day_open'] - 1
        data['high_pct'] = data['close'] / data['day_high'] - 1
        data['low_pct'] = data['close'] / data['day_low'] - 1
        data['VAMP_JC'] = CROSS(data['close'], data['VAMP'])
        data['VAMP_SC'] = CROSS(data['VAMP'], data['close'])
        data['VAMP_C'] = data.groupby(['date', 'code']).apply(lambda x: spc(x))['VAMP_C']
        data[['VAMP_DEGRESS','VAMP_SLOPE','VAMP_ATAN',
              'CLOSE_DEGRESS','CLOSE_SLOPE','CLOSE_ATAN']] = data.groupby(['date','code']).apply(lambda x: spc5(x))[['VAMP_DEGRESS','VAMP_SLOPE','VAMP_ATAN',
                                                                                                                     'CLOSE_DEGRESS','CLOSE_SLOPE','CLOSE_ATAN']]
    except:
        QA_util_log_info("JOB No {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = None

    return(data)


def QA_fetch_get_vwap(code, start_date, end_date, period='1', type='crawl'):

    data = QA_fetch_get_stock_vwap(code, start_date, end_date, period=period, type=type)
    data = data.groupby(['date','code']).agg({'VAMP_C':['min','max','mean','median','std','last',percentile(25),percentile(75)],
                                            'DISTANCE':['min','max','mean','median','std','last',percentile(25),percentile(75)]})
    data.columns = ['_'.join(col).strip().upper() for col in data.columns.values]
    data = data.reset_index()
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)