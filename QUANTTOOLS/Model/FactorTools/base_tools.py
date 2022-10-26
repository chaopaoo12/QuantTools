import pandas as pd
from scipy import stats as st
from matplotlib.pylab import *
import statsmodels.api as sml
import numpy as np
import math
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_index_info,QA_fetch_stock_industryinfo,QA_fetch_stock_all
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
    index_info = QA_fetch_index_info(index_code)
    stock_industry =QA_fetch_stock_industryinfo(list(QA_fetch_stock_all().code))
    block_name = QA.QA_fetch_stock_block()
    gn = block_name[(block_name.type == 'gn') & (block_name.source == 'tdx')]
    info = pd.DataFrame(QA.QAFetch.QAQuery.QA_fetch_stock_basic_info_tushare())

    try:
        a_list = list(index_info[index_info.cate == '2'].index)
        HY = list(index_info.loc[a_list]['HY'])
        HY_code = list(stock_industry[stock_industry['TDXHY'].isin(HY)]['code'])
    except:
        HY_code = []

    try:
        a_list = list(index_info[index_info.cate == '3'].index)
        index_name = list(index_info.loc[a_list]['index_name'].apply(lambda x:x.replace('板块', '')))
        area_code = list(info[info['area'].isin(index_name)]['code'])
    except:
        area_code = []

    try:
        a_list = list(index_info[index_info.cate == '4'].index)
        index_name = list(index_info.loc[a_list]['index_name'])
        gn_code = list(gn[gn['blockname'].isin(index_name)]['code'])
    except:
        gn_code = []

    try:
        a_list = list(index_info[index_info.cate == '8'].index)
        HY = list(index_info.loc[a_list]['HY'])
        SWHY1 = list(stock_industry[stock_industry['SWHY'].isin(HY)]['code'])
    except:
        SWHY1 = []

    try:
        a_list = list(index_info[(index_info.cate == '8' ) & (index_info.HY.str[4:6] == '00')].index)
        HY = list(index_info.loc[a_list]['HY'])
        SWHY2 = list(stock_industry[stock_industry['SWHY'].isin([i[0:4]+'01' for i in HY])]['code'])
    except:
        SWHY2 = []

    stock_code = HY_code + area_code + gn_code + SWHY1 + SWHY2
    return(stock_code)

def combine_model(index_d, stock_d, safe_d, start, end):
    res = pd.DataFrame()
    for i in QA.QAUtil.QA_util_get_trade_range(start,end):
        try:
            index_res = index_d[(index_d.y_pred==1) & (index_d.RANK<=5)].loc[i].sort_values(by='O_PROB', ascending=False)
        except:
            index_res = None

        try:
            safe_res = safe_d[(safe_d.y_pred==1) & (safe_d.RANK<=5)].loc[i]
        except:
            safe_res = None

        if index_res is not None:
            index_list = list(index_res.index)
        elif safe_res is not None:
            index_list = None
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
                    c = None

                if c is not None and c.shape[0] > 0:
                    c = c.assign(model_type = 1)
                    c = c.set_index(['date','code'])
                    res = res.append(c)

        elif safe_res is not None and safe_res.shape[0] > 0:
            c = stock_d.loc[i].sort_values(by='O_PROB', ascending=False).head(num).reset_index()

            if c is not None and c.shape[0] > 0:
                c = c.assign(date = i)
                c = c.assign(model_type = 2)

                res = res.append(c.set_index(['date','code']))

    return(res.drop_duplicates())

def combine_index(index_d, safe_d, start, end):
    res = pd.DataFrame()
    rngr = QA.QAUtil.QA_util_get_trade_range(start,end)
    if rngr is not None:

        for i in rngr:
            try:
                index_res = index_d[(index_d.y_pred==1) & (index_d.RANK<=5)].loc[i].sort_values(by='O_PROB', ascending=False)
            except:
                index_res = None

            try:
                safe_res = safe_d[(safe_d.y_pred==1) & (safe_d.RANK<=5)].loc[i]
            except:
                safe_res = None

            if index_res is not None:
                c = index_res
                model_type = 1
            elif index_res is None and safe_res is not None:
                c = safe_res
                model_type = 2
            else:
                c = None

            if c is not None and c.shape[0] > 0:
                c = c.assign(date = i)
                c = c.assign(model_type = model_type)
                c = c.reset_index()

                res = res.append(c.set_index(['date','code']))

        return(res.drop_duplicates())
    else:
        return(None)


def desribute_check(data, cols, drop):
    s_res = data[cols].describe().T
    s_res = s_res.assign(rate = s_res['count']/data.shape[0])
    non_cols = list(s_res[s_res.rate < drop].index)
    return(non_cols)

def thresh_chek(data, cols, thresh):
    nan_num = (data[cols].isnull().sum(axis=1)> 0).sum()
    #QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain NAN ==== '.format(
    #    NAN_NUM = nan_num, per=nan_num/data.shape[0], shape=data.shape[0]), ui_log = None)
    if thresh == 0:
        data = data[cols].dropna()
    else:
        data = data[cols].dropna(thresh=(len(cols) - thresh))

    #send_email('模型训练报告', "数据损失比例 {}".format(nan_num/train.shape[0]), self.info['date'])
    return (data, nan_num/data.shape[0])


def data_reshape(data, cols):

    cols1 = [i for i in data.columns if i not in [ 'moon','star','mars','venus','sun','MARK','date','datetime',
                                                   'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                   'TARGET4','TARGET5','TARGET10','TARGET20','AVG_TARGET','INDEX_TARGET',
                                                   'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                   'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]

    train = pd.DataFrame()
    n_cols = []
    for i in cols:
        if i in cols1:
            train[i] = data[i].astype('float')
        else:
            train[i] = 0
            n_cols.append(i)
    train.index = data.index

    return(train, n_cols)


def code_check(data):
    pass