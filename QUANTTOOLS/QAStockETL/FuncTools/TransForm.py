import numpy as np
import pandas as pd
import math
import statsmodels.api as sml

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

def standardize_series_rolling(series): #原始值法
    if (np.max(series) == 1 and np.min(series) == 0) or (np.max(series) == np.min(series)) :
        return(series[-1])
    else:
        std = np.std(series)
        mean = np.mean(series)
        return(round((series[-1]-mean)/std,4))

def normalization_series_rolling(series): #原始值法
    if series.max() ==1 and series.min() == 0:
        return(series[-1])
    elif series.max() ==0 and series.min() == 0:
        return(series[-1])
    elif series.max() == series.min():
        return(series.max()/series.min())
    else:
        return((series[-1]-series.min())/(series.max()-series.min()))

def filter_extreme_3sigma(array,n=3): #3 sigma
    try:
        array[array == -np.inf] = np.nan
        array[array == np.inf] = np.nan
    except:
        array = array.replace([np.inf, -np.inf], np.nan)
    array1 = array.astype(float)
    vmin = array1.min()
    vmax = array1.max()
    sigma = array1.std()
    mu = array1.mean()
    try:
        array[array == -np.inf] = vmax
        array[array == np.inf] = vmin
    except:
        array = array.replace(np.inf, vmin)
        array = array.replace(-np.inf, vmax)

    array = array.astype(float)
    array[array > mu + n*sigma] = mu + n*sigma
    array[array < mu - n*sigma] = mu - n*sigma
    return(array)

def neutralization(factor, mkt_cap=None, industry = None):
    y = factor
    if mkt_cap is not None:
        LnMktCap = mkt_cap
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
    res1 = data[[i for i in list(data.columns) if i not in ['INDUSTRY','TOTAL_MARKET','next_date']]].apply(lambda x:neutralization(normalization_series(filter_extreme_3sigma(x)), data['TOTAL_MARKET'], data['INDUSTRY']))
    return(res1)

def standardize(data):
    res1 = data[[i for i in list(data.columns) if i not in ['INDUSTRY','TOTAL_MARKET','next_date']]].apply(lambda x:neutralization(standardize_series(filter_extreme_3sigma(x)), data['TOTAL_MARKET'], data['INDUSTRY']))
    return(res1)

def normalization_rolling(data, N):
    res1 = data[[i for i in list(data.columns) if i not in ['INDUSTRY','TOTAL_MARKET','next_date']]].rolling(N).apply(lambda x:normalization_series_rolling(filter_extreme_3sigma(x)))
    return(res1)

def standardize_rolling(data, N):
    res1 = data[[i for i in list(data.columns) if i not in ['INDUSTRY','TOTAL_MARKET','next_date']]].rolling(N).apply(lambda x:standardize_series_rolling(filter_extreme_3sigma(x)))
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