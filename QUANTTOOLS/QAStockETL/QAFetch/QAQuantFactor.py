import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,QA_fetch_stock_alpha_adv,QA_fetch_stock_technical_index_adv,QA_fetch_stock_financial_percent_adv)
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
import QUANTAXIS as QA
import math
from QUANTTOOLS.QAStockETL.FuncTools.base_func import get_trans,series_to_supervised,time_this_function

@time_this_function
def QA_fetch_get_quant_data(codes, start_date, end_date):
    '获取股票量化机器学习最终指标V1'
    start = QA_util_get_pre_trade_date(start_date,61)
    rng1 = pd.Series(pd.date_range(start_date, end_date, freq='D')).apply(lambda x: str(x)[0:10])
    fianacial = QA_fetch_stock_fianacial_adv(codes,start,end_date).data[[ 'INDUSTRY','TOTAL_MARKET', 'TRA_RATE',
                                                                          'AVG5','AVG10','AVG20','AVG30','AVG60',
                                                                          'AVG5_TOR', 'AVG20_TOR','AVG30_TOR','AVG60_TOR',
                                                                          'GROSSMARGIN', 'GROSSMARGIN_L2Y','GROSSMARGIN_L3Y', 'GROSSMARGIN_L4Y', 'GROSSMARGIN_LY',
                                                                          'LAG5','LAG10','LAG20','LAG30','LAG60',
                                                                          'SZ50','HS300','CY300','SZ180','SZ380',
                                                                          'SZ100','SZ300','ZZ100','ZZ200','CY50',
                                                                          'NETCASHOPERATINRATE', 'NETCASHOPERATINRATE_L2Y', 'NETCASHOPERATINRATE_L3Y', 'NETCASHOPERATINRATE_LY',
                                                                          'NETPROFIT_INRATE', 'NETPROFIT_INRATE_L2Y', 'NETPROFIT_INRATE_L3Y', 'NETPROFIT_INRATE_LY',
                                                                          'OPERATINGRINRATE', 'OPERATINGRINRATE_L2Y', 'OPERATINGRINRATE_L3Y', 'OPERATINGRINRATE_LY',
                                                                          'PB', 'PBG', 'PC', 'PE', 'PEG', 'PM', 'PS','PSG','PT',
                                                                          'RNG_L','RNG_5','RNG_10','RNG_20', 'RNG_30', 'RNG_60',
                                                                          'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                                          'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                                          'AVG5_CR', 'AVG10_CR','AVG20_CR','AVG30_CR','AVG60_CR',
                                                                          'AVG5_TR','AVG10_TR','AVG20_TR','AVG30_TR','AVG60_TR',
                                                                          'TOTALPROFITINRATE', 'TOTALPROFITINRATE_L2Y', 'TOTALPROFITINRATE_L3Y', 'TOTALPROFITINRATE_LY']].groupby('code').fillna(method='ffill')
    perank = QA_fetch_stock_financial_percent_adv(codes,start,end_date).data.groupby('code').fillna(method='ffill')
    fianacial = fianacial.join(perank)
    alpha = QA_fetch_stock_alpha_adv(codes,start,end_date).data[['alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
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
                                                                 'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191']].groupby('code').apply(series_to_supervised,[10,7,5,3,1]).loc[rng1].groupby('date').apply(get_trans)
    for columnname in alpha.columns:
        if alpha[columnname].dtype == 'float64':
            alpha[columnname]=alpha[columnname].astype('float16')
        if alpha[columnname].dtype == 'int64':
            alpha[columnname]=alpha[columnname].astype('int8')
    technical = QA_fetch_stock_technical_index_adv(codes,start,end_date).data.astype(float).groupby('code').apply(series_to_supervised,[10,7,5,4,3,2,1]).loc[rng1].groupby('date').apply(get_trans)
    for columnname in technical.columns:
        if technical[columnname].dtype == 'float64':
            technical[columnname]=technical[columnname].astype('float16')
        if technical[columnname].dtype == 'int64':
            technical[columnname]=technical[columnname].astype('int8')
    fianacial['TOTAL_MARKET']= fianacial['TOTAL_MARKET'].apply(lambda x:math.log(x))
    cols = [i for i in list(fianacial.columns) if i not in ['INDUSTRY','TOTAL_MARKET',
                                                            'SZ50','HS300','CY300','SZ180','SZ380',
                                                            'SZ100','SZ300','ZZ100','ZZ200','CY50']]
    fianacial = fianacial[cols].groupby('code').apply(series_to_supervised,[10,7,5,3,1]).loc[rng1].join(fianacial.loc[rng1][['SZ50','HS300','CY300','SZ180','SZ380',
                                                                                                                                'SZ100','SZ300','ZZ100','ZZ200','CY50',
                                                                                                                                'INDUSTRY','TOTAL_MARKET']])
    fianacial = fianacial[[x for x in list(fianacial.columns) if x not in ['INDUSTRY','TOTAL_MARKET','SZ50','HS300','CY300','SZ180','SZ380',
                                                                           'SZ100','SZ300','ZZ100','ZZ200','CY50']]].groupby('date').apply(get_trans).join(fianacial[['INDUSTRY','TOTAL_MARKET','SZ50','HS300','CY300','SZ180','SZ380',
                                                                                                                                                                      'SZ100','SZ300','ZZ100','ZZ200','CY50']])
    for columnname in fianacial.columns:
        if fianacial[columnname].dtype == 'float64':
            fianacial[columnname]=fianacial[columnname].astype('float16')
        if fianacial[columnname].dtype == 'int64':
            fianacial[columnname]=fianacial[columnname].astype('int8')
    res = fianacial.join(technical).join(alpha).fillna(0).reset_index()
    res = res.assign(date_stamp=res['date'].apply(lambda x: str(x)[0:10]))
    return(res)