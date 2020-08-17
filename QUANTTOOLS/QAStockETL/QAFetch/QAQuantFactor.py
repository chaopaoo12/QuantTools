import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,QA_fetch_stock_alpha_adv,QA_fetch_stock_technical_index_adv,QA_fetch_stock_financial_percent_adv,
                                                           QA_fetch_index_alpha_adv,QA_fetch_index_technical_index_adv,QA_fetch_stock_alpha101_adv,QA_fetch_index_alpha101_adv)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_index_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp, QA_util_log_info,QA_util_get_trade_range,
                               QA_util_get_pre_trade_date)
import math
from QUANTTOOLS.QAStockETL.QAUtil.base_func import normalization, standardize, time_this_function

def QA_fetch_index_cate(data, stock_code):
    try:
        return data.loc[stock_code]['cate']
    except:
        return None

@time_this_function
def QA_fetch_get_index_quant_data(codes, start_date, end_date, type='standardize', ui_log = None):
    '获取股票量化机器学习最终指标V1'
    start = QA_util_get_pre_trade_date(start_date,15)
    QA_util_log_info(
        '##JOB got index quant data date range ============== from {from_} to {to_} '.format(from_=start,to_=end_date), ui_log)
    rng1 = QA_util_get_trade_range(start_date, end_date)
    QA_util_log_info(
        '##JOB got Data index alpha191 data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    alpha = QA_fetch_index_alpha_adv(codes,start,end_date).data[['alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
                                                                 'alpha_009', 'alpha_010', 'alpha_012', 'alpha_013', 'alpha_014', 'alpha_015', 'alpha_016', 'alpha_017',
                                                                 'alpha_018', 'alpha_019', 'alpha_020', 'alpha_021', 'alpha_022', 'alpha_023', 'alpha_024', 'alpha_025', 'alpha_026',
                                                                 'alpha_028', 'alpha_029', 'alpha_031', 'alpha_032', 'alpha_033', 'alpha_034', 'alpha_035', 'alpha_036', 'alpha_037',
                                                                 'alpha_038', 'alpha_039', 'alpha_040', 'alpha_041', 'alpha_042', 'alpha_044', 'alpha_045', 'alpha_046',
                                                                 'alpha_047', 'alpha_048', 'alpha_049', 'alpha_052', 'alpha_053', 'alpha_054', 'alpha_055', 'alpha_056', 'alpha_057',
                                                                 'alpha_058', 'alpha_059', 'alpha_061', 'alpha_062', 'alpha_063', 'alpha_064', 'alpha_065', 'alpha_066',
                                                                 'alpha_067', 'alpha_068', 'alpha_071', 'alpha_072', 'alpha_074', 'alpha_077', 'alpha_078', 'alpha_080',
                                                                 'alpha_082', 'alpha_083', 'alpha_085', 'alpha_086', 'alpha_087', 'alpha_088', 'alpha_089',
                                                                 'alpha_090', 'alpha_091', 'alpha_092', 'alpha_093', 'alpha_096', 'alpha_098', 'alpha_099',
                                                                 'alpha_102', 'alpha_103', 'alpha_104', 'alpha_105', 'alpha_106', 'alpha_107', 'alpha_108', 'alpha_109',
                                                                 'alpha_113', 'alpha_114', 'alpha_115', 'alpha_116', 'alpha_117', 'alpha_118', 'alpha_119', 'alpha_120',
                                                                 'alpha_122', 'alpha_123', 'alpha_124', 'alpha_125', 'alpha_126', 'alpha_129', 'alpha_130', 'alpha_133',
                                                                 'alpha_134', 'alpha_135', 'alpha_138', 'alpha_139', 'alpha_141', 'alpha_142', 'alpha_145', 'alpha_148',
                                                                 'alpha_152', 'alpha_153', 'alpha_156', 'alpha_158', 'alpha_159', 'alpha_160', 'alpha_161', 'alpha_162',
                                                                 'alpha_163', 'alpha_164', 'alpha_167', 'alpha_168', 'alpha_169', 'alpha_170', 'alpha_171', 'alpha_172', 'alpha_173',
                                                                 'alpha_175', 'alpha_176', 'alpha_177', 'alpha_178', 'alpha_179', 'alpha_184', 'alpha_185', 'alpha_186',
                                                                 'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191']].loc[rng1]
    QA_util_log_info(
        '##JOB got Data index alpha101 data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    alpha101 = QA_fetch_index_alpha101_adv(codes,start,end_date).data.loc[rng1]
    alphas = alpha.join(alpha101)
    QA_util_log_info(
        '##JOB got Data index tech data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    technical = QA_fetch_index_technical_index_adv(codes,start,end_date).data.drop(['PBX1','PBX1_C','PBX2','PBX2_C','PBX3','PBX3_C','PBX4','PBX4_C','PBX5','PBX5_C','PBX6','PBX6_C','PBX_STD','PVT','PVT_C'], axis=1).loc[rng1]
    QA_util_log_info(
        '##JOB got Data index tech week data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    tech_week = QA_fetch_index_technical_index_adv(codes,start,end_date, 'week').data.drop(['PBX1','PBX1_C','PBX2','PBX2_C','PBX3','PBX3_C','PBX4','PBX4_C','PBX5','PBX5_C','PBX6','PBX6_C','PBX_STD','PVT','PVT_C'], axis=1).loc[rng1]
    tech_week.columns = [x + '_WK' for x in tech_week.columns]
    technical = technical.join(tech_week)
    QA_util_log_info(
        '##JOB index quant data combine ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    res = technical.join(alphas)
    QA_util_log_info(
        '##JOB index quant data trans ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    if type == 'standardize':
        res = res[[x for x in list(res.columns) if x not in col_tar]].groupby('date').apply(standardize).reset_index()
    elif type == 'normalization':
        res = res[[x for x in list(res.columns) if x not in col_tar]].groupby('date').apply(normalization).reset_index()
    else:
        res = res.reset_index()
        QA_util_log_info('##JOB type must be in [standardize, normalization]', ui_log)

    cate = QA_fetch_index_info(codes)
    res = res.assign(cate=res['code'].apply(lambda x: str(QA_fetch_index_cate(cate, str(x)))))
    res = res.assign(date_stamp=res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(res)

@time_this_function
def QA_fetch_get_quant_data(codes, start_date, end_date, type='standardize', ui_log = None):
    '获取股票量化机器学习最终指标V1'
    start = QA_util_get_pre_trade_date(start_date,0)
    QA_util_log_info(
        '##JOB got stock quant data date range ============== from {from_} to {to_} '.format(from_=start,to_=end_date), ui_log)
    rng1 = QA_util_get_trade_range(start_date, end_date)
    QA_util_log_info(
        '##JOB got Data stock fianacial data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    fianacial = QA_fetch_stock_fianacial_adv(codes,start,end_date).data[[ 'INDUSTRY','TOTAL_MARKET', 'TRA_RATE', 'DAYS',
                                                                          'AVG5','AVG10','AVG20','AVG30','AVG60',
                                                                          'LAG','LAG5','LAG10','LAG20','LAG30','LAG60',
                                                                          'AVG5_TOR', 'AVG20_TOR','AVG30_TOR','AVG60_TOR',
                                                                          'GROSSMARGIN','NETPROFIT_INRATE','OPERATINGRINRATE','NETCASHOPERATINRATE',
                                                                          'PB', 'PBG', 'PC', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PM', 'PS','PSG','PT',
                                                                          'I_PB','I_PE','I_PEEGL','I_ROE','I_ROE_TOTAL','I_ROA','I_ROA_TOTAL','I_GROSSMARGIN',
                                                                          'PE_RATE','PEEGL_RATE','PB_RATE','ROE_RATE','ROE_RATET','ROA_RATE','ROA_RATET',
                                                                          'GROSS_RATE','ROA_AVG5','ROE_AVG5','GROSS_AVG5','ROE_MIN','ROA_MIN','GROSS_MIN',
                                                                          'ROE_CH','ROA_CH','GROSS_CH','OPINRATE_AVG3','NETPINRATE_AVG3',
                                                                          'RNG','RNG_L','RNG_5','RNG_10','RNG_20', 'RNG_30', 'RNG_60',
                                                                          'AVG5_RNG','AVG10_RNG','AVG20_RNG','AVG30_RNG','AVG60_RNG',
                                                                          'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                                          'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                                          'AVG5_CR', 'AVG10_CR','AVG20_CR','AVG30_CR','AVG60_CR',
                                                                          'AVG5_TR','AVG10_TR','AVG20_TR','AVG30_TR','AVG60_TR','TOTALPROFITINRATE']]
    fianacial = fianacial[fianacial.DAYS >= 90]
    QA_util_log_info(
        '##JOB got Data stock perank data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    perank = QA_fetch_stock_financial_percent_adv(codes,start,end_date).data
    fianacial = fianacial.join(perank).groupby('code').fillna(method='ffill')
    QA_util_log_info(
        '##JOB got Data stock alpha191 data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    alpha = QA_fetch_stock_alpha_adv(codes,start,end_date).data[["alpha_001","alpha_002","alpha_003","alpha_004","alpha_005","alpha_006","alpha_007","alpha_008",
                                                                 "alpha_009","alpha_010","alpha_011","alpha_012","alpha_013","alpha_014","alpha_015","alpha_016",
                                                                 "alpha_017","alpha_018","alpha_019","alpha_020","alpha_021","alpha_022","alpha_023","alpha_024",
                                                                 "alpha_025","alpha_026","alpha_028","alpha_029","alpha_031","alpha_032","alpha_033","alpha_034",
                                                                 "alpha_035","alpha_036","alpha_037","alpha_038","alpha_039","alpha_040","alpha_041","alpha_042",
                                                                 "alpha_043","alpha_044","alpha_045","alpha_046","alpha_047","alpha_048","alpha_049","alpha_052",
                                                                 "alpha_053","alpha_054","alpha_055","alpha_056","alpha_057","alpha_058","alpha_059","alpha_060",
                                                                 "alpha_061","alpha_062","alpha_063","alpha_064","alpha_065","alpha_066","alpha_067","alpha_068",
                                                                 "alpha_070","alpha_071","alpha_072","alpha_074","alpha_076","alpha_077","alpha_078","alpha_079",
                                                                 "alpha_080","alpha_081","alpha_082","alpha_083","alpha_084","alpha_085","alpha_086","alpha_087",
                                                                 "alpha_088","alpha_089","alpha_090","alpha_091","alpha_092","alpha_093","alpha_094","alpha_095",
                                                                 "alpha_096","alpha_097","alpha_098","alpha_099","alpha_100","alpha_101","alpha_102","alpha_103",
                                                                 "alpha_104","alpha_105","alpha_106","alpha_107","alpha_108","alpha_109","alpha_110","alpha_111",
                                                                 "alpha_112","alpha_113","alpha_114","alpha_115","alpha_116","alpha_117","alpha_118","alpha_119",
                                                                 "alpha_120","alpha_122","alpha_123","alpha_124","alpha_125","alpha_126","alpha_127","alpha_128",
                                                                 "alpha_129","alpha_130","alpha_132","alpha_133","alpha_134","alpha_135","alpha_136","alpha_137",
                                                                 "alpha_138","alpha_139","alpha_141","alpha_142","alpha_144","alpha_145","alpha_146","alpha_147",
                                                                 "alpha_148","alpha_149","alpha_150","alpha_152","alpha_153","alpha_154","alpha_155","alpha_156",
                                                                 "alpha_157","alpha_158","alpha_159","alpha_160","alpha_161","alpha_162","alpha_163","alpha_164",
                                                                 "alpha_167","alpha_168","alpha_169","alpha_170","alpha_171","alpha_172","alpha_173","alpha_174",
                                                                 "alpha_175","alpha_176","alpha_177","alpha_178","alpha_179","alpha_180","alpha_184","alpha_185",
                                                                 "alpha_186","alpha_187","alpha_188","alpha_189","alpha_191"]].loc[rng1]
    QA_util_log_info(
        '##JOB got Data stock alpha101 data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    alpha101 = QA_fetch_stock_alpha101_adv(codes,start,end_date).data.loc[rng1]
    alphas = alpha.join(alpha101).groupby('code').fillna(method='ffill')
    for columnname in alphas.columns:
        if alphas[columnname].dtype == 'float64':
            alphas[columnname]=alphas[columnname].astype('float16')
        if alphas[columnname].dtype == 'int64':
            alphas[columnname]=alphas[columnname].astype('int8')
    QA_util_log_info(
        '##JOB got Data stock tech data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    technical = QA_fetch_stock_technical_index_adv(codes,start,end_date).data.drop(['PBX1','PBX1_C','PBX2','PBX2_C','PBX3','PBX3_C','PBX4','PBX4_C','PBX5','PBX5_C','PBX6','PBX6_C','PBX_STD','PVT','PVT_C'], axis=1).loc[rng1]
    QA_util_log_info(
        '##JOB got Data stock tech week data ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    tech_week = QA_fetch_stock_technical_index_adv(codes,start,end_date, 'week').data.drop(['PBX1','PBX1_C','PBX2','PBX2_C','PBX3','PBX3_C','PBX4','PBX4_C','PBX5','PBX5_C','PBX6','PBX6_C','PBX_STD','PVT','PVT_C'], axis=1).loc[rng1]
    tech_week.columns = [x + '_WK' for x in tech_week.columns]
    technical = technical.join(tech_week).groupby('code').fillna(method='ffill')
    for columnname in technical.columns:
        if technical[columnname].dtype == 'float64':
            technical[columnname]=technical[columnname].astype('float16')
        if technical[columnname].dtype == 'int64':
            technical[columnname]=technical[columnname].astype('int8')
    fianacial['FINA_VAL']= fianacial['NETPROFIT_INRATE']/fianacial['ROE']
    fianacial['RNG60_RES']= (fianacial['AVG60_RNG']*60) / fianacial['RNG_60']
    fianacial['RNG20_RES']= (fianacial['AVG60_RNG']*20) / fianacial['RNG_20']
    fianacial['TOTAL_MARKET']= fianacial['TOTAL_MARKET'].apply(lambda x:math.log(x))
    fianacial = fianacial.loc[rng1]
    for columnname in fianacial.columns:
        if fianacial[columnname].dtype == 'float64':
            fianacial[columnname]=fianacial[columnname].astype('float16')
        if fianacial[columnname].dtype == 'int64':
            fianacial[columnname]=fianacial[columnname].astype('int8')
    QA_util_log_info(
        '##JOB stock quant data combine ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    res = fianacial.join(technical).join(alphas)
    col_tar = ['DAYS','INDUSTRY']
    QA_util_log_info(
        '##JOB stock quant data trans ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    if type == 'standardize':
        res = res[[x for x in list(res.columns) if x not in col_tar]].groupby('date').apply(standardize).join(res[col_tar])
    elif type == 'normalization':
        res = res[[x for x in list(res.columns) if x not in col_tar]].groupby('date').apply(normalization).join(res[col_tar])
    else:
        pass
        QA_util_log_info('##JOB type must be in [standardize, normalization]', ui_log)

    QA_util_log_info(
        '##JOB got Data stock industry info ============== from {from_} to {to_} '.format(from_= start_date,to_=end_date), ui_log)
    res = res.reset_index()
    res = res.assign(date_stamp=res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(res)