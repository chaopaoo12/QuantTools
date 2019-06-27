import numpy as np
import pandas as pd
import time
import math

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
    if series.max() ==1 and series.min == 0:
        return(series)
    else:
        return((series-series.min)/(series.max-series.min))

def filter_extreme_3sigma(array,n=3): #3 sigma
    array1 = array.replace([np.inf, -np.inf], np.nan)
    vmax = array1.max()
    vmin = array1.min()
    if vmax ==1 and vmin == 0:
        return(array1)
    else:
        sigma = array1.std()
        mu = array1.mean()
        array = array.replace(np.inf, vmin)
        array = array.replace(-np.inf, vmax)
        array[array > mu + n*sigma] = mu + n*sigma
        array[array < mu - n*sigma] = mu - n*sigma
        return(array)

# 去极值，标准化，中性化
def neutralization(factor,mkt_cap = True, industry = True):
    y = factor
    if type(mkt_cap) == pd.Series:
        LnMktCap = mkt_cap.apply(lambda x:math.log(x))
        if industry: #行业、市值
            dummy_industry = pd.get_dummies(factor.index)
            x = pd.concat([LnMktCap,dummy_industry.T],axis = 1)
        else: #仅市值
            x = LnMktCap
    elif type(industry) == pd.Series: #仅行业
        dummy_industry = pd.get_dummies(factor.index)
        x = dummy_industry.T
    result = sml.OLS(y.astype(float),x.astype(float)).fit()
    return result.resid

def get_trans(data):
    return(data.apply(filter_extreme_3sigma).apply(standardize_series))

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
    if type == 'close':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data['PRE_MARKET']= data['close_qfq'].shift(-1).apply(lambda x:round(x * 100,2))
        data['PRE2_MARKET']= data['close_qfq'].shift(-2).apply(lambda x:round(x * 100,2))
        data['PRE3_MARKET']= data['close_qfq'].shift(-3).apply(lambda x:round(x * 100,2))
        data['PRE5_MARKET']= data['close_qfq'].shift(-5).apply(lambda x:round(x * 100,2))
        data['AVG_PRE_MARKET']= data['AVG_TOTAL_MARKET'].shift(-1).apply(lambda x:round(x * 100,2))
        data['AVG_PRE2_MARKET']= data['AVG_TOTAL_MARKET'].shift(-2).apply(lambda x:round(x * 100,2))
        data['AVG_PRE3_MARKET']= data['AVG_TOTAL_MARKET'].shift(-3).apply(lambda x:round(x * 100,2))
        data['AVG_PRE5_MARKET']= data['AVG_TOTAL_MARKET'].shift(-5).apply(lambda x:round(x * 100,2))
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = data['AVG_TOTAL_MARKET'].pct_change(1).shift(-1).apply(lambda x:round(x * 100,2))
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data['PRE_MARKET']= data['high_qfq'].shift(-1).apply(lambda x:round(x * 100,2))
        data['PRE2_MARKET']= data['high_qfq'].shift(-2).apply(lambda x:round(x * 100,2))
        data['PRE3_MARKET']= data['high_qfq'].shift(-3).apply(lambda x:round(x * 100,2))
        data['PRE5_MARKET']= data['high_qfq'].shift(-5).apply(lambda x:round(x * 100,2))
        data['AVG_PRE_MARKET']= data['AVG_TOTAL_MARKET'].shift(-1).apply(lambda x:round(x * 100,2))
        data['AVG_PRE2_MARKET']= data['AVG_TOTAL_MARKET'].shift(-2).apply(lambda x:round(x * 100,2))
        data['AVG_PRE3_MARKET']= data['AVG_TOTAL_MARKET'].shift(-3).apply(lambda x:round(x * 100,2))
        data['AVG_PRE5_MARKET']= data['AVG_TOTAL_MARKET'].shift(-5).apply(lambda x:round(x * 100,2))
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = data['AVG_TOTAL_MARKET'].pct_change(1).shift(-1).apply(lambda x:round(x * 100,2))
    else:
        data=None
    return(data)

def index_pct(market):
    market['PRE_MARKET']= market['close'].shift(-1)
    market['PRE2_MARKET']= market['close'].shift(-2)
    market['PRE3_MARKET']= market['close'].shift(-3)
    market['PRE5_MARKET']= market['close'].shift(-5)
    market['INDEX_TARGET'] = (market['PRE2_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET3'] = (market['PRE3_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET5'] = (market['PRE5_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    return(market)