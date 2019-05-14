import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,QA_fetch_stock_alpha_adv,QA_fetch_stock_technical_index_adv)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_basic_info_tushare
import math
import numpy as np

def standardize_series(series): #原始值法
    std = series.std()
    mean = series.mean()
    return(round((series-mean)/std * 100,4))

def filter_extreme_3sigma(array,n=3): #3 sigma
    sigma = array.std()
    mu = array.mean()
    array[array > mu + n*sigma] = mu + n*sigma
    array[array < mu - n*sigma] = mu - n*sigma
    return(array)

def get_trans(data):
    return(data.apply(filter_extreme_3sigma).apply(standardize_series))

def series_to_supervised(data, n_in=[1], n_out=1, fill = True, dropnan=True):
    cols_na = list(data.columns)
    if fill == True:
        df = pd.DataFrame(data).fillna(method='ffill')
    else:
        df = pd.DataFrame(data)
    cols, names = list(), list()
    # input sequence (t-n, ... t-1)
    for i in n_in:
        cols.append(df.shift(i))
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

def get_quant_data(start_date, end_date):


    fianacial = QA_fetch_stock_fianacial_adv(list(QA_fetch_stock_list_adv()['code']),start_date,end_date).data[[ 'INDUSTRY','TOTAL_MARKET', 'TRA_RATE',
                                                                                                     'AVG5', 'AVG20','AVG30','AVG60',
                                                                                                     'AVG5_TOR', 'AVG20_TOR','AVG30_TOR','AVG60_TOR',
                                                                                                     'GROSSMARGIN', 'GROSSMARGIN_L2Y','GROSSMARGIN_L3Y', 'GROSSMARGIN_L4Y', 'GROSSMARGIN_LY',
                                                                                                     'LAG20', 'LAG30', 'LAG5', 'LAG60',
                                                                                                     'NETCASHOPERATINRATE', 'NETCASHOPERATINRATE_L2Y', 'NETCASHOPERATINRATE_L3Y', 'NETCASHOPERATINRATE_LY',
                                                                                                     'NETPROFIT_INRATE', 'NETPROFIT_INRATE_L2Y', 'NETPROFIT_INRATE_L3Y', 'NETPROFIT_INRATE_LY',
                                                                                                     'OPERATINGRINRATE', 'OPERATINGRINRATE_L2Y', 'OPERATINGRINRATE_L3Y', 'OPERATINGRINRATE_LY',
                                                                                                     'PB', 'PBG', 'PC', 'PE', 'PEG', 'PM', 'PS', 'PSC', 'PSG', 'PT',
                                                                                                     'RNG_20', 'RNG_30', 'RNG_5', 'RNG_60', 'RNG_L',
                                                                                                     'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                                                                     'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                                                                     'TOTALPROFITINRATE', 'TOTALPROFITINRATE_L2Y', 'TOTALPROFITINRATE_L3Y', 'TOTALPROFITINRATE_LY',
                                                                                                     'TARGET', 'TARGET3', 'TARGET5']]
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
                                                                                                        'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191']].groupby('date').apply(get_trans).groupby('code').apply(series_to_supervised,[30,10,7,5,3,1])
    technical = QA_fetch_stock_technical_index_adv(list(QA_fetch_stock_list_adv()['code']),start_date,end_date).data.groupby('date').apply(get_trans).groupby('code').apply(series_to_supervised,[10,7,5,4,3,2,1])
    fianacial['TOTAL_MARKET']= fianacial['TOTAL_MARKET'].apply(lambda x:math.log(x))
    cols = [i for i in list(fianacial.columns) if i not in ['GROSSMARGIN', 'GROSSMARGIN_L2Y', 'GROSSMARGIN_L3Y', 'GROSSMARGIN_L4Y', 'GROSSMARGIN_LY',
                                                        'NETCASHOPERATINRATE', 'NETCASHOPERATINRATE_L2Y', 'NETCASHOPERATINRATE_L3Y', 'NETCASHOPERATINRATE_LY',
                                                        'NETPROFIT_INRATE', 'NETPROFIT_INRATE_L2Y', 'NETPROFIT_INRATE_L3Y', 'NETPROFIT_INRATE_LY',
                                                        'OPERATINGRINRATE', 'OPERATINGRINRATE_L2Y', 'OPERATINGRINRATE_L3Y', 'OPERATINGRINRATE_LY',
                                                        'TOTALPROFITINRATE', 'TOTALPROFITINRATE_L2Y', 'TOTALPROFITINRATE_L3Y', 'TOTALPROFITINRATE_LY',
                                                        'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                        'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                        'TARGET', 'TARGET3', 'TARGET5','INDUSTRY','TOTAL_MARKET']]
    fianacial = fianacial[cols].groupby('code').apply(series_to_supervised,[30,10,7,5,3,1]).join(fianacial[['GROSSMARGIN', 'GROSSMARGIN_L2Y', 'GROSSMARGIN_L3Y', 'GROSSMARGIN_L4Y', 'GROSSMARGIN_LY',
                                                                                                            'NETCASHOPERATINRATE', 'NETCASHOPERATINRATE_L2Y', 'NETCASHOPERATINRATE_L3Y', 'NETCASHOPERATINRATE_LY',
                                                                                                            'NETPROFIT_INRATE', 'NETPROFIT_INRATE_L2Y', 'NETPROFIT_INRATE_L3Y', 'NETPROFIT_INRATE_LY',
                                                                                                            'OPERATINGRINRATE', 'OPERATINGRINRATE_L2Y', 'OPERATINGRINRATE_L3Y', 'OPERATINGRINRATE_LY',
                                                                                                            'TOTALPROFITINRATE', 'TOTALPROFITINRATE_L2Y', 'TOTALPROFITINRATE_L3Y', 'TOTALPROFITINRATE_LY',
                                                                                                            'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                                                                            'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                                                                            'TARGET', 'TARGET3', 'TARGET5','INDUSTRY','TOTAL_MARKET']])
    fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['TARGET', 'TARGET3', 'TARGET5','INDUSTRY','TOTAL_MARKET']]].groupby('date').apply(get_trans).join(fianacial[['TARGET', 'TARGET3', 'TARGET5','INDUSTRY','TOTAL_MARKET']])
    res = fianacial.join(technical).join(alpha)
    return(res)