from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq, QA_fetch_get_stock_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_min_adv
import datetime
import time
from scipy import stats
import pandas as pd
import numpy as np
import math
from QUANTAXIS.QAIndicator.base import CROSS,EMA,MA
import random

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

def rolling_k(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(round(model.slope/model.intercept*100,4))

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
    data[['VAMPC_K']]= data.rolling(window=N,min_periods=2).agg({'VAMP':rolling_k})
    return(data)

#def spc5(data, N= 5):
#    data[['VAMP_C5',
#          'close_c5']]= data.rolling(window=N,min_periods=5).agg({'VAMP':rolling_ols,
#                                                                'close':rolling_ols})
#    return(data)

def spc5(data, N= 5):
    data[['VAMP_K','CLOSE_K']]= data.rolling(window=N,min_periods=3).agg({'VAMP':rolling_k,'close':rolling_k})
    return(data)

def spcc5(data, N= 5):
    data[['camt_k']]= data.rolling(window=N,min_periods=3).agg({'camt_vol':rolling_k})
    return(data)

def sohlc(data, N= 240):
    data[['day_open','day_close','day_high','day_low']] = data.rolling(window=N, min_periods=1).agg({'open':first,'close':last,'high':'max','low':'min'})
    return(data)

def QA_fetch_get_stock_vwap(code, start_date, end_date, period = '1', type = 'crawl',proxies=[]):
    QA_util_log_info("JOB Get {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))


    start_time = time.time()
    if type == 'crawl':
        data = QA_fetch_stock_min_adv(code, start_date, end_date, frequence='1min').data
    elif type == 'real':
        data = QA_fetch_get_stock_min_sina(code=code, period=period, type='qfq',proxies=proxies)
        #data = QA_fetch_get_usstock_day_xq(code, start_date, end_date, period='1m')
    print("0 --- %s seconds ---" % (time.time() - start_time))
    if data is not None and type == 'real':
        data = data.reset_index(drop=True).set_index(['datetime', 'code']).drop(columns=['date_stamp'])

    try:
        start_time = time.time()
        data = data.assign(date=data.reset_index().datetime.apply(lambda x:str(x)[0:10]).tolist(),
                           HM=data.reset_index().datetime.dt.strftime('%H:%M').values,
                           )
        print("1 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        data = data.assign(camt=data.groupby(['date','code'])['amount'].cumsum(),
                           cvolume=data.groupby(['date','code'])['volume'].cumsum(),
                           duration=data['HM'].apply(lambda x: (datetime.datetime.strptime('15:00','%H:%M') - datetime.datetime.strptime(x,'%H:%M')).total_seconds()/60))
        print("2 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        data[['open_p','close_p','high_p','low_p','AMT_P','VOL_P']] = \
            data.groupby(['date','code'])[['open','close','high','low','camt','cvolume']].shift()
        data[['open_p2','close_p2','high_p2','low_p2']] = \
            data.groupby(['date','code'])[['open','close','high','low']].shift(2)
        data[['AMT_P','VOL_P']] = data.groupby(['HM','code'])[['camt','cvolume']].shift()
        print("3 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        data['AMT_UP'] = data['camt'] / data['AMT_P'] - 1
        if type == 'crawl':
            data['VAMP'] = data['camt'] / data['cvolume']
        else:
            data['VAMP'] = data['camt'] / data['cvolume'] / 100
        data['DISTANCE'] = data['close'] / data['VAMP'] - 1
        print("4 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        #data['camt_vol'] = data['camt'] / ((data.groupby('code')['camt'].shift(241*2) + data.groupby('code')['camt'].shift(241*3) + data.groupby('code')['camt'].shift(241)) /3)
        print("5 --- %s seconds ---" % (time.time() - start_time))

        #data['camt_k'] = data.groupby(['date', 'code']).apply(lambda x: spcc5(x))[['camt_k']]
        print("6 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        #data[['day_open', 'day_close', 'day_high', 'day_low']] = data.groupby(['date','code']).apply(lambda x: sohlc(x))[['day_open', 'day_close', 'day_high', 'day_low']]
        print("7 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        #data['open_pct'] = data['close'] / data['day_open'] - 1
        #data['high_pct'] = data['close'] / data['day_high'] - 1
        #data['low_pct'] = data['close'] / data['day_low'] - 1
        data['EMA'] = EMA(data['close'], 9)
        data['VAMP_JC'] = CROSS(data['close'], data['VAMP'])
        data['VAMP_SC'] = CROSS(data['VAMP'], data['close'])
        print("8 --- %s seconds ---" % (time.time() - start_time))
        start_time = time.time()
        #data[['VAMPC_K']] = data.groupby(['date', 'code']).apply(lambda x: spc(x))[['VAMPC_K']]
        #data[['VAMP_K','CLOSE_K']] = data.groupby(['date','code']).apply(lambda x: spc5(x))[['VAMP_K','CLOSE_K']]
        print("9 --- %s seconds ---" % (time.time() - start_time))

    except:
        QA_util_log_info("JOB No {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = None

    return(data)


def QA_fetch_get_vwap(code, start_date, end_date, period='1', type='crawl'):

    data = QA_fetch_get_stock_vwap(code, start_date, end_date, period=period, type=type)
    data = data.groupby(['date','code']).agg({'VAMP_K':['min','max','mean','median','std','last',percentile(25),percentile(75)],
                                              'VAMPC_K':['min','max','mean','median','std','last',percentile(25),percentile(75)],
                                            'DISTANCE':['min','max','mean','median','std','last',percentile(25),percentile(75)]})
    data.columns = ['_'.join(col).strip().upper() for col in data.columns.values]
    data = data.reset_index()
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)