import pandas as pd
from scipy import stats as st
from matplotlib.pylab import *
import statsmodels.api as sml
import numpy as np
import math
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry
import QUANTAXIS as QA

def standardize_series(series): #原始值法
    std = series.std()
    mean = series.mean()
    return (series-mean)/std

def filter_extreme_MAD(series,n): #MAD:中位数去极值
    median = np.percentile(series,50)
    new_median = np.percentile((series - median).abs(),50)
    max_range = median + n*new_median
    min_range = median - n*new_median
    return np.clip(series,min_range,max_range)

def filter_extreme_3sigma(series,n=3): #3 sigma
    mean = series.mean()
    std = series.std()
    max_range = mean + n*std
    min_range = mean - n*std
    return np.clip(series,min_range,max_range)

def filter_extreme_percentile(series,min = 0.025,max = 0.975): #百分位法
    series = series.sort_values()
    q = series.quantile([min,max])
    return np.clip(series,q.iloc[0],q.iloc[1])

# 去极值，标准化，中性化
def neutralization(factor,mkt_cap = True, industry = True):
    y = factor
    if type(mkt_cap) == pd.Series:
        LnMktCap = mkt_cap.apply(lambda x:math.log(x))
        if industry: #行业、市值
            dummy_industry = QA_fetch_stock_industry(factor.index)
            x = pd.concat([LnMktCap,dummy_industry.T],axis = 1)
        else: #仅市值
            x = LnMktCap
    elif type(industry) == pd.Series: #仅行业
        dummy_industry = pd.get_dummies(industry)
        x = dummy_industry.T
    result = sml.OLS(y.astype(float),x.astype(float)).fit()
    return result.resid

#IC计算
def get_factor_ic(factor, returns):
    factor.index = pd.to_datetime(factor.index)
    ic_data = pd.DataFrame(index=returns.index, columns=['IC','pValue'])

    # 计算相关系数
    for dt in ic_data.index:
        tmp_factor = factor.ix[dt]
        tmp_ret = returns.ix[dt]
        cor = pd.DataFrame(tmp_factor)
        ret = pd.DataFrame(tmp_ret)
        cor.columns = ['corr']
        ret.columns = ['ret']
        cor['ret'] = ret['ret']
        cor = cor[~np.isnan(cor['corr'])][~np.isnan(cor['ret'])]
        if len(cor) < 5:
            continue

        ic, p_value = st.spearmanr(cor['corr'],cor['ret'])   # 计算秩相关系数 RankIC
        ic_data['IC'][dt] = ic
        ic_data['pValue'][dt] = p_value
    return ic_data

def halflife_ic(IC_data, halflife_weight,name, T):
    ic = []
    for i in range(len(IC_data)-T):
        pre_date = IC_data.index[i]
        date = IC_data.index[i+T-1]
        data = np.sum(IC_data.loc[pre_date:date,'IC'].values*halflife_weight)
        ic.append(data)
    return pd.DataFrame(ic, index=IC_data.index[T:], columns=[name])

def find_stock(index_code):
    stock = QA.QA_fetch_stock_block()
    index_list = QA.QA_fetch_index_list_adv()
    block = index_list.loc[index_code]['name']
    if isinstance(index_code,list):
        stock_code = stock[stock['blockname'].isin(block)]['code']
    else:
        stock_code = list(stock[stock['blockname'] == block]['code'])
    return(stock_code)

def combine_model(index_d, stock_d, safe_d, start, end):
    res = pd.DataFrame()
    for i in QA.QAUtil.QA_util_get_trade_range(start,end):
        try:
            index_res = index_d[index_d['y_pred']==1][index_d['RANK']<=5].loc[i].sort_values(by='O_PROB', ascending=False)
        except:
            index_res = None

        try:
            safe_res = safe_d[safe_d['y_pred']==1][safe_d['RANK']<=5].loc[i]
        except:
            safe_res = None

        if index_res is not None:
            index_list = list(index_res.index)
        #elif safe_res is not None:
        #   index_list = list(safe_res.index)
        else:
            index_list = None

        if index_list is None:
            num = 5
        elif len(index_list) == 1:
            num = 5
        elif len(index_list) == 2:
            num = 3
        elif len(index_list) == 3:
            num = 2
        else:
            num = 1

        if index_list is not None:
            for j in index_list:
                try:
                    c = stock_d.loc[(i,find_stock(j)),:].sort_values(by='O_PROB', ascending=False).head(num).reset_index()
                except:
                    if safe_res is not None and safe_res.shape[0] > 0:
                        c = stock_d.loc[i].sort_values(by='O_PROB', ascending=False).head(num).reset_index()
                    else:
                        c = pd.DataFrame()

                if c is not None and c.shape[0] > 0:
                    res = res.reset_index().append(c,ignore_index=True).set_index(['date','code'])
        else:
            pass

    return(res.drop_duplicates())