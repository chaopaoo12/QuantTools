import pandas as pd
from scipy import stats as st
from matplotlib.pylab import *
import statsmodels.api as sml
import numpy as np
import math
from QUANTTOOLS import QA_fetch_stock_fianacial_adv,QA_fetch_stock_alpha_adv
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_basic_info_tushare

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
def neutralization(factor,mkt_cap = False, industry = True):
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

def get_data(start_date, end_date):
    alpha = QA_fetch_stock_alpha_adv(list(QA_fetch_stock_list_adv()['code']),start_date,end_date).data[['alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
                                                                                                        'alpha_009', 'alpha_010', 'alpha_011', 'alpha_012', 'alpha_013', 'alpha_014', 'alpha_015', 'alpha_016', 'alpha_017',
                                                                                                        'alpha_018', 'alpha_019', 'alpha_020', 'alpha_021', 'alpha_022', 'alpha_023', 'alpha_024', 'alpha_025', 'alpha_026',
                                                                                                        'alpha_028', 'alpha_029', 'alpha_031', 'alpha_032', 'alpha_033', 'alpha_034', 'alpha_035', 'alpha_036', 'alpha_037',
                                                                                                        'alpha_038', 'alpha_039', 'alpha_040', 'alpha_041', 'alpha_042', 'alpha_043', 'alpha_044', 'alpha_045', 'alpha_046',
                                                                                                        'alpha_047', 'alpha_048', 'alpha_049', 'alpha_052', 'alpha_053', 'alpha_054', 'alpha_055', 'alpha_056', 'alpha_057',
                                                                                                        'alpha_058', 'alpha_059', 'alpha_060', 'alpha_061', 'alpha_062', 'alpha_063', 'alpha_064', 'alpha_065', 'alpha_066',
                                                                                                        'alpha_067', 'alpha_068', 'alpha_070', 'alpha_071', 'alpha_072', 'alpha_074', 'alpha_077', 'alpha_078', 'alpha_080',
                                                                                                        'alpha_081', 'alpha_082', 'alpha_083', 'alpha_084', 'alpha_085', 'alpha_086', 'alpha_087', 'alpha_088', 'alpha_089',
                                                                                                        'alpha_090', 'alpha_091', 'alpha_092', 'alpha_093', 'alpha_095', 'alpha_096', 'alpha_097', 'alpha_098', 'alpha_099',
                                                                                                        'alpha_100', 'alpha_102', 'alpha_103', 'alpha_104', 'alpha_105', 'alpha_106', 'alpha_107', 'alpha_108', 'alpha_109',
                                                                                                        'alpha_111', 'alpha_113', 'alpha_114', 'alpha_115', 'alpha_116', 'alpha_117', 'alpha_118', 'alpha_119', 'alpha_120',
                                                                                                        'alpha_122', 'alpha_123', 'alpha_124', 'alpha_125', 'alpha_126', 'alpha_129', 'alpha_130', 'alpha_132', 'alpha_133',
                                                                                                        'alpha_134', 'alpha_135', 'alpha_138', 'alpha_139', 'alpha_141', 'alpha_142', 'alpha_145', 'alpha_148', 'alpha_150',
                                                                                                        'alpha_152', 'alpha_153', 'alpha_155', 'alpha_156', 'alpha_158', 'alpha_159', 'alpha_160', 'alpha_161', 'alpha_162',
                                                                                                        'alpha_163', 'alpha_164', 'alpha_167', 'alpha_168', 'alpha_169', 'alpha_170', 'alpha_171', 'alpha_172', 'alpha_173',
                                                                                                        'alpha_175', 'alpha_176', 'alpha_177', 'alpha_178', 'alpha_179', 'alpha_180', 'alpha_184', 'alpha_185', 'alpha_186',
                                                                                                        'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191','date','code']]
    financial = QA_fetch_stock_fianacial_adv(list(QA_fetch_stock_list_adv()['code']),'2018-01-01','2018-12-31').data
    stock_list = pd.DataFrame(QA_fetch_stock_basic_info_tushare())[['code','industry']]
    res = pd.merge(pd.merge(alpha,financial,how='right',left_on=['code','date'],right_on=['CODE','date']),
                   stock_list,how='left',left_on=['code'],right_on = ['code'])
    res['moon'] =  res['TARGET3'].apply(lambda x: 1 if x > 3 else 0)
    res['sun'] =  res['TARGET5'].apply(lambda x: 1 if x > 3 else 0)
    res1 = res[['alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
                'alpha_009', 'alpha_010', 'alpha_011', 'alpha_012', 'alpha_013', 'alpha_014', 'alpha_015', 'alpha_016', 'alpha_017',
                'alpha_018', 'alpha_019', 'alpha_020', 'alpha_021', 'alpha_022', 'alpha_023', 'alpha_024', 'alpha_025', 'alpha_026',
                'alpha_028', 'alpha_029', 'alpha_031', 'alpha_032', 'alpha_033', 'alpha_034', 'alpha_035', 'alpha_036', 'alpha_037',
                'alpha_038', 'alpha_039', 'alpha_040', 'alpha_041', 'alpha_042', 'alpha_043', 'alpha_044', 'alpha_045', 'alpha_046',
                'alpha_047', 'alpha_048', 'alpha_049', 'alpha_052', 'alpha_053', 'alpha_054', 'alpha_055', 'alpha_056', 'alpha_057',
                'alpha_058', 'alpha_059', 'alpha_060', 'alpha_061', 'alpha_062', 'alpha_063', 'alpha_064', 'alpha_065', 'alpha_066',
                'alpha_067', 'alpha_068', 'alpha_070', 'alpha_071', 'alpha_072', 'alpha_074', 'alpha_077', 'alpha_078', 'alpha_080',
                'alpha_081', 'alpha_082', 'alpha_083', 'alpha_084', 'alpha_085', 'alpha_086', 'alpha_087', 'alpha_088', 'alpha_089',
                'alpha_090', 'alpha_091', 'alpha_092', 'alpha_093', 'alpha_095', 'alpha_096', 'alpha_097', 'alpha_098', 'alpha_099',
                'alpha_100', 'alpha_102', 'alpha_103', 'alpha_104', 'alpha_105', 'alpha_106', 'alpha_107', 'alpha_108', 'alpha_109',
                'alpha_111', 'alpha_113', 'alpha_114', 'alpha_115', 'alpha_116', 'alpha_117', 'alpha_118', 'alpha_119', 'alpha_120',
                'alpha_122', 'alpha_123', 'alpha_124', 'alpha_125', 'alpha_126', 'alpha_129', 'alpha_130', 'alpha_132', 'alpha_133',
                'alpha_134', 'alpha_135', 'alpha_138', 'alpha_139', 'alpha_141', 'alpha_142', 'alpha_145', 'alpha_148', 'alpha_150',
                'alpha_152', 'alpha_153', 'alpha_155', 'alpha_156', 'alpha_158', 'alpha_159', 'alpha_160', 'alpha_161', 'alpha_162',
                'alpha_163', 'alpha_164', 'alpha_167', 'alpha_168', 'alpha_169', 'alpha_170', 'alpha_171', 'alpha_172', 'alpha_173',
                'alpha_175', 'alpha_176', 'alpha_177', 'alpha_178', 'alpha_179', 'alpha_180', 'alpha_184', 'alpha_185', 'alpha_186',
                'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191','date','code','industry','PB', 'PBG', 'PC', 'PE', 'PEG', 'PM', 'PS', 'PSC', 'PSG',
                'PT','moon','sun','TARGET','TARGET3','TARGET5']]
    res1['date'] = pd.to_datetime(res1['date'])
    return(res1)