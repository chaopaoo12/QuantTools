import numpy as np
import pandas as pd
import time
import math
import statsmodels.api as sml

def time_this_function(func):
    #作为装饰器使用，返回函数执行需要花费的时间
    def inner(*args,**kwargs):
        start=time.time()
        result=func(*args,**kwargs)
        end=time.time()
        print(func.__name__,end-start)
        return result
    return inner

def standardize_series(series): #原始值法
    if (np.max(series) == 1 and np.min(series) == 0) or (np.max(series) == np.min(series)) :
        return(series)
    else:
        std = np.std(series)
        mean = np.mean(series)
        return(round((series-mean)/std,4))

def normalization_series(series): #原始值法
    if series.max() ==1 and series.min() == 0:
        return(series)
    elif series.max() ==0 and series.min() == 0:
        return(series)
    elif series.max() == series.min():
        return(series.max()/series.min())
    else:
        return((series-series.min())/(series.max()-series.min()))

def filter_extreme_3sigma(array,n=3): #3 sigma
    array1 = array.replace([np.inf, -np.inf], np.nan).astype(float)
    vmin = array1.min()
    vmax = array1.max()
    sigma = array1.std()
    mu = array1.mean()
    array1 = array.replace(np.inf, vmin).astype(float)
    array = array1.replace(-np.inf, vmax).astype(float)
    array[array > mu + n*sigma] = mu + n*sigma
    array[array < mu - n*sigma] = mu - n*sigma
    return(array)

def neutralization(factor, mkt_cap=None, industry = None):
    y = factor
    if mkt_cap is not None:
        LnMktCap = mkt_cap.apply(lambda x:math.log(x))
        if industry is not None: #行业、市值
            dummy_industry = pd.get_dummies(industry).astype(float)
            x = pd.concat([LnMktCap,dummy_industry],axis = 1)
        else: #仅市值
            x = LnMktCap
    elif industry is not None: #仅行业
        dummy_industry = pd.get_dummies(industry).astype(float)
        x = dummy_industry
    result = sml.OLS(y.astype(float),x.astype(float)).fit()
    return result.resid

def normalization(data):
    res1 = data[[i for i in list(data.columns) if i != 'INDUSTRY']].apply(lambda x:normalization_series(filter_extreme_3sigma(x)))
    return(res1)

def standardize(data):
    res1 = data[[i for i in list(data.columns) if i != 'INDUSTRY']].apply(lambda x:standardize_series(filter_extreme_3sigma(x)))
    return(res1)

def series_to_supervised(data, n_in=[1], n_out=1, fill = True, dropnan=True):
    cols_na = list(data.columns)
    if fill == True:
        try:
            df = pd.DataFrame(data).fillna(method='ffill')
        except:
            df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(data)
    cols, names = list(), list()
    # input sequence (t-n, ... t-1)
    for i in n_in:
        cols.append(df.diff(i))
        names += [('%s(t-%d)' % (j, i)) for j in cols_na]
    # forecast sequence (t, t+1, ... t+n)
    #print(range(0, n_out))
    for i in range(0, n_out):
        cols.append(df.shift(-i))
        if i == 0:
            names += [('%s(t)' % (j)) for j in cols_na]
        else:
            names += [('%s(t+%d)' % (j, i)) for j in cols_na]
    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    if dropnan:
        agg.dropna(how='all',inplace=True)
    return agg

def pct(data, type = 'close'):
    data = data.sort_values('date',ascending=True)
    if type == 'close':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET','high_mark','low_mark']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
        data[['PRE_DATE','PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['date','close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['close_qfq','AVG_TOTAL_MARKET']]
        data['OPEN_MARK'] = (data['high_mark'] == data['low_mark']) * 1
        data['PASS_MARK'] = (data['PRE_MARKET']/data['close_qfq']-1).apply(lambda x:round(x * 100,2))
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET4'] = (data['PRE4_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET10'] = (data['PRE10_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = (data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET']-1).apply(lambda x:round(x * 100,2))
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET4'] = (data['PRE4_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET10'] = (data['PRE10_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = (data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET']-1).apply(lambda x:round(x * 100,2))
    else:
        data=None
    return(data)

def index_pct(market):
    market = market.sort_values('date',ascending=True)
    market['PRE_MARKET']= market.shift(-1)['close']
    market['PRE2_MARKET']= market.shift(-2)['close']
    market['PRE3_MARKET']= market.shift(-3)['close']
    market['PRE4_MARKET']= market.shift(-4)['close']
    market['PRE5_MARKET']= market.shift(-5)['close']
    market['PRE10_MARKET']= market.shift(-10)['close']
    market['INDEX_TARGET'] = (market['PRE2_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET3'] = (market['PRE3_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET4'] = (market['PRE4_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET5'] = (market['PRE5_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET10'] = (market['PRE10_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    return(market)

def pct_log(data, type = 'close'):
    data = data.sort_values('date',ascending=True)
    if type == 'close':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET','high_mark','low_mark']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
        data[['PRE_DATE','PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['date','close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['close_qfq','AVG_TOTAL_MARKET']]
        data['OPEN_MARK'] = (data['high_mark'] == data['low_mark']) * 1
        data['PASS_MARK'] = (data['PRE_MARKET']/data['close_qfq']-1).apply(lambda x:round(x * 100,2))
        data['TARGET'] = np.log(data['PRE2_MARKET']/data['PRE_MARKET'])
        data['TARGET3'] = np.log(data['PRE3_MARKET']/data['PRE_MARKET'])
        data['TARGET4'] = np.log(data['PRE4_MARKET']/data['PRE_MARKET'])
        data['TARGET5'] = np.log(data['PRE5_MARKET']/data['PRE_MARKET'])
        data['TARGET10'] = np.log(data['PRE10_MARKET']/data['PRE_MARKET'])
        data['AVG_TARGET'] = np.log(data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET'])
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = np.log(data['PRE2_MARKET']/data['PRE_MARKET'])
        data['TARGET3'] = np.log(data['PRE3_MARKET']/data['PRE_MARKET'])
        data['TARGET4'] = np.log(data['PRE4_MARKET']/data['PRE_MARKET'])
        data['TARGET5'] = np.log(data['PRE5_MARKET']/data['PRE_MARKET'])
        data['TARGET10'] = np.log(data['PRE10_MARKET']/data['PRE_MARKET'])
        data['AVG_TARGET'] = np.log(data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET'])
    else:
        data=None
    return(data)

def index_pct_log(market):
    market = market.sort_values('date',ascending=True)
    market['PRE_MARKET']= market.shift(-1)['close']
    market['PRE2_MARKET']= market.shift(-2)['close']
    market['PRE3_MARKET']= market.shift(-3)['close']
    market['PRE4_MARKET']= market.shift(-4)['close']
    market['PRE5_MARKET']= market.shift(-5)['close']
    market['PRE10_MARKET']= market.shift(-10)['close']
    market['INDEX_TARGET'] = np.log(market['PRE2_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET3'] = np.log(market['PRE3_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET4'] = np.log(market['PRE4_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET5'] = np.log(market['PRE5_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET10'] = np.log(market['PRE10_MARKET']/market['PRE_MARKET'])
    return(market)