
import pymongo
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_quant_data,QA_fetch_get_index_quant_data
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_log_info,
                              QA_util_get_trade_range,QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv,QA_fetch_index_list_adv

def QA_SU_save_stock_quant_day(code=None, start_date=None,end_date=None, ui_log = None, ui_progress = None):
    if start_date is None:
        if end_date is None:
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
            end_date = QA_util_today_str()
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])

    financial = DATABASE.stock_quant_data_financial
    financial.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    index = DATABASE.stock_quant_data_index
    index.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    week = DATABASE.stock_quant_data_week
    week.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    alpha = DATABASE.stock_quant_data_alpha
    alpha.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    try:
        data1 = QA_fetch_get_quant_data(code, start_date, end_date)
    except:
        data1 = None
    else:
        QA_util_log_info(
            '##JOB got Data stock quant data ============== from {from_} to {to_} '.format(from_=start_date,to_=end_date), ui_log)
    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        print('not a trading day')
    elif data1 is None:
        print('not a trading day')
    else:
        for deal_date in deal_date_list:
            if QA_util_if_trade(deal_date):
                data = data1[data1['date']==deal_date]
            else:
                data = None
            if data is not None:
                data = data.drop_duplicates(
                    (['code', 'date']))

                financial_data = data[['code','date','date_stamp','INDUSTRY','TOTAL_MARKET', 'TRA_RATE', 'DAYS',
                                       'AVG5','AVG10','AVG20','AVG30','AVG60',
                                       'LAG','LAG5','LAG10','LAG20','LAG30','LAG60',
                                       'LAG_TOR','AVG5_TOR', 'AVG20_TOR','AVG30_TOR','AVG60_TOR',
                                       'GROSSMARGIN', 'GROSSMARGIN_L2Y','GROSSMARGIN_L3Y', 'GROSSMARGIN_L4Y', 'GROSSMARGIN_LY',
                                       'NETCASHOPERATINRATE', 'NETCASHOPERATINRATE_L2Y', 'NETCASHOPERATINRATE_L3Y', 'NETCASHOPERATINRATE_LY',
                                       'NETPROFIT_INRATE', 'NETPROFIT_INRATE_L2Y', 'NETPROFIT_INRATE_L3Y', 'NETPROFIT_INRATE_LY',
                                       'OPERATINGRINRATE', 'OPERATINGRINRATE_L2Y', 'OPERATINGRINRATE_L3Y', 'OPERATINGRINRATE_LY',
                                       'PB', 'PBG', 'PC', 'PE', 'PEG', 'PM', 'PS','PSG','PT',
                                       'RNG','RNG_L','RNG_5','RNG_10','RNG_20', 'RNG_30', 'RNG_60',
                                       'AVG5_RNG','AVG10_RNG','AVG20_RNG','AVG30_RNG','AVG60_RNG',
                                       'ROA', 'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                       'ROE', 'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                       'AVG5_CR', 'AVG10_CR','AVG20_CR','AVG30_CR','AVG60_CR',
                                       'AVG5_TR','AVG10_TR','AVG20_TR','AVG30_TR','AVG60_TR',
                                       'TOTALPROFITINRATE', 'TOTALPROFITINRATE_L2Y', 'TOTALPROFITINRATE_L3Y', 'TOTALPROFITINRATE_LY', 'PBRANK_10VAL',
                                       'PBRANK_20VAL','PBRANK_30VAL','PBRANK_60VAL','PBRANK_90VAL',
                                       'PB_10PCT','PB_10VAL','PB_20PCT','PB_20VAL',
                                       'PB_30PCT','PB_30VAL','PB_60PCT','PB_60VAL',
                                       'PB_90PCT','PB_90VAL','PEG_10PCT','PEG_10VAL',
                                       'PEG_20PCT','PEG_20VAL','PEG_30PCT','PEG_30VAL',
                                       'PEG_60PCT','PEG_60VAL','PEG_90PCT','PEG_90VAL',
                                       'PERANK_10VAL','PERANK_20VAL','PERANK_30VAL','PERANK_60VAL','PERANK_90VAL',
                                       'PE_10PCT','PE_10VAL','PE_20PCT','PE_20VAL',
                                       'PE_30PCT','PE_30VAL','PE_60PCT','PE_60VAL',
                                       'PE_90PCT','PE_90VAL','PS_10PCT','PS_10VAL',
                                       'PS_20PCT','PS_20VAL','PS_30PCT','PS_30VAL',
                                       'PS_60PCT','PS_60VAL','PS_90PCT','PS_90VAL',
                                       'FINA_VAL','RNG60_RES','RNG20_RES', 'RNG_LO','LAG_TORO','DAYSO']]
                alpha_data = data[['code','date','date_stamp','alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
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
                                   'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191']]
                index_data = data[['code','date','date_stamp','AD','ADDI','ADDI_C','ADTM','ADX','ADXR','ADX_C','AD_C',
                                   'AMA','ASI','ASIT','ATR','ATRR','BBI','BIAS1','BIAS2',
                                   'BIAS3','BODY','BODY_ABS','BOLL','CCI','CHO','DDD','DDI',
                                   'DDI_C','DEA','DI1','DI2','DIF','DI_M','KDJ_D','KDJ_J',
                                   'KDJ_K','LB','MA1','MA10','MA120','MA180','MA2','MA20',
                                   'MA3','MA4','MA5','MA60','MAADTM','MACD','MACHO','MAOSC',
                                   'MAVPT','MFI','MFI_C','MIKE_BOLL','MR','MS','MTM','MTMMA',
                                   'OBV','OBV_C','OSC','PRICE_PCG','ROC','ROCMA','RSI1','RSI1_C',
                                   'RSI2','RSI2_C','RSI3','RSI3_C','RSV','SHA_LOW','SHA_UP',
                                   'SKDJ_D','SKDJ_K','SR','SS','TR','UB','VPT','VR','VRSI',
                                   'VRSI_C','VSTD','WIDTH','WR','WR1','WR2','WS','CCI_CROSS4',
                                   'DMA_CROSS1','CDLMORNINGDOJISTAR','CDLSEPARATINGLINES',
                                   'WR_CROSS1','KDJ_CROSS2','CDLHARAMICROSS','CDLEVENINGSTAR',
                                   'BBI_CROSS2','VPT_CROSS1','CROSS_SC','CDLSHORTLINE',
                                   'SKDJ_CROSS1','CDLABANDONEDBABY','CDL3STARSINSOUTH',
                                   'CDLUNIQUE3RIVER','CDLKICKINGBYLENGTH','CDLHOMINGPIGEON',
                                   'CDLTAKURI','CDL3BLACKCROWS','CDLSTICKSANDWICH','CDLTASUKIGAP',
                                   'VPT_CROSS2','CDLSHOOTINGSTAR','CDLCONCEALBABYSWALL','WR_CROSS2',
                                   'ADTM_CROSS1','BIAS_CROSS2','MTM_CROSS4','CCI_CROSS3','CDLHAMMER',
                                   'CDLMARUBOZU','MACD_TR','CDL3INSIDE','CDLUPSIDEGAP2CROWS',
                                   'MTM_CROSS1','CDLGRAVESTONEDOJI','KDJ_CROSS1','CDLMATHOLD',
                                   'MIKE_TR','CDLLADDERBOTTOM','CDLMORNINGSTAR','OSC_CROSS2',
                                   'OSC_CROSS4','ADX_CROSS2','DI_CROSS1','MTM_CROSS2','CDLDRAGONFLYDOJI',
                                   'CCI_CROSS2','CDLSPINNINGTOP','CDLHIKKAKEMOD','DMA_CROSS2',
                                   'MIKE_WRJC','CROSS_JC','OSC_CROSS3','RSI_CROSS1','MIKE_WSJC',
                                   'MTM_CROSS3','CDLADVANCEBLOCK','BIAS_CROSS1','CDLCLOSINGMARUBOZU',
                                   'CDL3OUTSIDE','VPT_CROSS3','CDLEVENINGDOJISTAR','CDL2CROWS',
                                   'CDLHANGINGMAN','ADTM_CROSS2','CDLMATCHINGLOW','CDLHIKKAKE',
                                   'CDLKICKING','CDLCOUNTERATTACK','CHO_CROSS1','CDLHARAMI',
                                   'BBI_CROSS1','MIKE_WRSC','CDLINVERTEDHAMMER','CCI_CROSS1',
                                   'CDLBREAKAWAY','CDLGAPSIDESIDEWHITE','DI_CROSS2','CDL3WHITESOLDIERS',
                                   'CDLTRISTAR','CDLXSIDEGAP3METHODS','CDLPIERCING','VPT_CROSS4',
                                   'CDLLONGLINE','CDLDOJI','CDLHIGHWAVE','CDLSTALLEDPATTERN',
                                   'ADX_CROSS1','CDL3LINESTRIKE','CDLBELTHOLD','CDLINNECK',
                                   'CDLONNECK','CDLRICKSHAWMAN','CDLTHRUSTING','CDLIDENTICAL3CROWS',
                                   'SKDJ_CROSS2','CDLDOJISTAR','RSI_CROSS2','OSC_CROSS1',
                                   'CDLRISEFALL3METHODS','CDLLONGLEGGEDDOJI','MIKE_WSSC',
                                   'CDLDARKCLOUDCOVER','CHO_CROSS2','CDLENGULFING']]
                week_data = data[['code','date','date_stamp','AD_WK','ADDI_WK','ADDI_C_WK','ADTM_WK','ADX_WK','ADXR_WK','ADX_C_WK','AD_C_WK',
                                  'AMA_WK','ASI_WK','ASIT_WK','ATR_WK','ATRR_WK','BBI_WK','BIAS1_WK','BIAS2_WK',
                                  'BIAS3_WK','BODY_WK','BODY_ABS_WK','BOLL_WK','CCI_WK','CHO_WK','DDD_WK','DDI_WK',
                                  'DDI_C_WK','DEA_WK','DI1_WK','DI2_WK','DIF_WK','DI_M_WK','KDJ_D_WK','KDJ_J_WK',
                                  'KDJ_K_WK','LB_WK','MA1_WK','MA10_WK','MA120_WK','MA180_WK','MA2_WK','MA20_WK',
                                  'MA3_WK','MA4_WK','MA5_WK','MA60_WK','MAADTM_WK','MACD_WK','MACHO_WK','MAOSC_WK',
                                  'MAVPT_WK','MFI_WK','MFI_C_WK','MIKE_BOLL_WK','MR_WK','MS_WK','MTM_WK','MTMMA_WK',
                                  'OBV_WK','OBV_C_WK','OSC_WK','PRICE_PCG_WK','ROC_WK','ROCMA_WK','RSI1_WK','RSI1_C_WK',
                                  'RSI2_WK','RSI2_C_WK','RSI3_WK','RSI3_C_WK','RSV_WK','SHA_LOW_WK','SHA_UP_WK','SKDJ_D_WK',
                                  'SKDJ_K_WK','SR_WK','SS_WK','TR_WK','UB_WK','VPT_WK','VR_WK','VRSI_WK','VRSI_C_WK','VSTD_WK',
                                  'WIDTH_WK','WR_WK','WR1_WK','WR2_WK','WS_WK','CDLDRAGONFLYDOJI_WK','MIKE_WRJC_WK',
                                  'CDLRICKSHAWMAN_WK','MIKE_WSSC_WK','DI_CROSS2_WK','CDLHARAMI_WK','BBI_CROSS2_WK',
                                  'VPT_CROSS2_WK','CDLBELTHOLD_WK','CDLHAMMER_WK','CDL3INSIDE_WK','CDLTRISTAR_WK',
                                  'OSC_CROSS1_WK','CDLMARUBOZU_WK','CDLTASUKIGAP_WK','CDLSPINNINGTOP_WK','CDLDARKCLOUDCOVER_WK',
                                  'CDL3BLACKCROWS_WK','BIAS_CROSS2_WK','OSC_CROSS3_WK','CHO_CROSS1_WK','CDLMORNINGSTAR_WK',
                                  'ADX_CROSS2_WK','CDLINNECK_WK','ADTM_CROSS2_WK','MACD_TR_WK','CDLDOJI_WK','MTM_CROSS1_WK',
                                  'CDLCOUNTERATTACK_WK','CDLLONGLINE_WK','KDJ_CROSS1_WK','CDLADVANCEBLOCK_WK','CDLHANGINGMAN_WK',
                                  'KDJ_CROSS2_WK','ADX_CROSS1_WK','CDLMATHOLD_WK','CDLABANDONEDBABY_WK','WR_CROSS2_WK',
                                  'MIKE_WRSC_WK','OSC_CROSS2_WK','CDLGAPSIDESIDEWHITE_WK','CROSS_JC_WK','MTM_CROSS4_WK',
                                  'CDLSHOOTINGSTAR_WK','ADTM_CROSS1_WK','CDL3OUTSIDE_WK','CDLLONGLEGGEDDOJI_WK','CDL3LINESTRIKE_WK',
                                  'CDLHIKKAKE_WK','CDLSTALLEDPATTERN_WK','MTM_CROSS2_WK','SKDJ_CROSS2_WK','CDLEVENINGDOJISTAR_WK',
                                  'OSC_CROSS4_WK','CDLTAKURI_WK','CDLSHORTLINE_WK','CROSS_SC_WK','CDLMATCHINGLOW_WK',
                                  'CCI_CROSS4_WK','MIKE_WSJC_WK','CDLHOMINGPIGEON_WK','VPT_CROSS1_WK','CDLCLOSINGMARUBOZU_WK',
                                  'WR_CROSS1_WK','CDLTHRUSTING_WK','BBI_CROSS1_WK','DMA_CROSS2_WK','RSI_CROSS1_WK',
                                  'CDLRISEFALL3METHODS_WK','CDLHIKKAKEMOD_WK','CCI_CROSS3_WK','CDLKICKINGBYLENGTH_WK',
                                  'CDLLADDERBOTTOM_WK','DI_CROSS1_WK','VPT_CROSS3_WK','CDLHARAMICROSS_WK','CHO_CROSS2_WK',
                                  'CCI_CROSS2_WK','CDL3STARSINSOUTH_WK','CDLXSIDEGAP3METHODS_WK','RSI_CROSS2_WK',
                                  'MIKE_TR_WK','CDLDOJISTAR_WK','CDLCONCEALBABYSWALL_WK','CDLPIERCING_WK','CDLHIGHWAVE_WK',
                                  'CDLMORNINGDOJISTAR_WK','CDLSTICKSANDWICH_WK','CDLGRAVESTONEDOJI_WK','CDLINVERTEDHAMMER_WK',
                                  'CDLKICKING_WK','CDLSEPARATINGLINES_WK','CDLBREAKAWAY_WK','MTM_CROSS3_WK','CDLUNIQUE3RIVER_WK',
                                  'CCI_CROSS1_WK','DMA_CROSS1_WK','VPT_CROSS4_WK','CDLEVENINGSTAR_WK','CDL2CROWS_WK',
                                  'CDL3WHITESOLDIERS_WK','CDLIDENTICAL3CROWS_WK','CDLUPSIDEGAP2CROWS_WK','CDLENGULFING_WK',
                                  'SKDJ_CROSS1_WK','BIAS_CROSS1_WK','CDLONNECK_WK']]
                QA_util_log_info(
                    '##JOB01 Pre Data stock quant data ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                financial_res = QA_util_to_json_from_pandas(financial_data)
                alpha_res = QA_util_to_json_from_pandas(alpha_data)
                index_res = QA_util_to_json_from_pandas(index_data)
                week_res = QA_util_to_json_from_pandas(week_data)
                QA_util_log_info(
                    '##JOB02 Got Data stock quant data ============== {deal_date}'.format(deal_date=deal_date), ui_log)
                try:
                    financial.insert_many(financial_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock quant data financial saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        financial.insert_many(financial_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass

                try:
                    alpha.insert_many(alpha_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock quant data alpha saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        alpha.insert_many(alpha_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass

                try:
                    week.insert_many(week_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock quant data week saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        week.insert_many(week_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass

                try:
                    index.insert_many(index_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock quant data index saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        index.insert_many(index_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
            else:
                QA_util_log_info(
                    '##JOB01 No Data stock_quant_datat ============== {deal_date} '.format(deal_date=deal_date), ui_log)


def QA_SU_save_index_quant_day(code=None, start_date=None,end_date=None, ui_log = None, ui_progress = None):
    if start_date is None:
        if end_date is None:
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
            end_date = QA_util_today_str()
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')
    if code is None:
        code = list(QA_fetch_index_list_adv()['code'])

    index = DATABASE.index_quant_data_index
    index.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    week = DATABASE.index_quant_data_week
    week.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    alpha = DATABASE.index_quant_data_alpha
    alpha.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    try:
        data1 = QA_fetch_get_index_quant_data(code, start_date, end_date)
    except:
        data1 = None
    else:
        QA_util_log_info(
            '##JOB got Data index quant data ============== from {from_} to {to_} '.format(from_=start_date,to_=end_date), ui_log)
    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        print('not a trading day')
    elif data1 is None:
        print('not a trading day')
    else:
        for deal_date in deal_date_list:
            if QA_util_if_trade(deal_date):
                data = data1[data1['date']==deal_date]
            else:
                data = None
            if data is not None:
                data = data.drop_duplicates(
                    (['code', 'date']))
                alpha_data = data[['code','date','date_stamp','alpha_001', 'alpha_002', 'alpha_003', 'alpha_004', 'alpha_005', 'alpha_006', 'alpha_007', 'alpha_008',
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
                                   'alpha_187', 'alpha_188', 'alpha_189', 'alpha_191']]
                index_data = data[['code','date','date_stamp','AD','ADDI','ADDI_C','ADTM','ADX','ADXR','ADX_C','AD_C',
                                   'AMA','ASI','ASIT','ATR','ATRR','BBI','BIAS1','BIAS2',
                                   'BIAS3','BODY','BODY_ABS','BOLL','CCI','CHO','DDD','DDI',
                                   'DDI_C','DEA','DI1','DI2','DIF','DI_M','KDJ_D','KDJ_J',
                                   'KDJ_K','LB','MA1','MA10','MA120','MA180','MA2','MA20',
                                   'MA3','MA4','MA5','MA60','MAADTM','MACD','MACHO','MAOSC',
                                   'MAVPT','MFI','MFI_C','MIKE_BOLL','MR','MS','MTM','MTMMA',
                                   'OBV','OBV_C','OSC','PRICE_PCG','ROC','ROCMA','RSI1','RSI1_C',
                                   'RSI2','RSI2_C','RSI3','RSI3_C','RSV','SHA_LOW','SHA_UP',
                                   'SKDJ_D','SKDJ_K','SR','SS','TR','UB','VPT','VR','VRSI',
                                   'VRSI_C','VSTD','WIDTH','WR','WR1','WR2','WS','CCI_CROSS4',
                                   'DMA_CROSS1','CDLMORNINGDOJISTAR','CDLSEPARATINGLINES',
                                   'WR_CROSS1','KDJ_CROSS2','CDLHARAMICROSS','CDLEVENINGSTAR',
                                   'BBI_CROSS2','VPT_CROSS1','CROSS_SC','CDLSHORTLINE',
                                   'SKDJ_CROSS1','CDLABANDONEDBABY','CDL3STARSINSOUTH',
                                   'CDLUNIQUE3RIVER','CDLKICKINGBYLENGTH','CDLHOMINGPIGEON',
                                   'CDLTAKURI','CDL3BLACKCROWS','CDLSTICKSANDWICH','CDLTASUKIGAP',
                                   'VPT_CROSS2','CDLSHOOTINGSTAR','CDLCONCEALBABYSWALL','WR_CROSS2',
                                   'ADTM_CROSS1','BIAS_CROSS2','MTM_CROSS4','CCI_CROSS3','CDLHAMMER',
                                   'CDLMARUBOZU','MACD_TR','CDL3INSIDE','CDLUPSIDEGAP2CROWS',
                                   'MTM_CROSS1','CDLGRAVESTONEDOJI','KDJ_CROSS1','CDLMATHOLD',
                                   'MIKE_TR','CDLLADDERBOTTOM','CDLMORNINGSTAR','OSC_CROSS2',
                                   'OSC_CROSS4','ADX_CROSS2','DI_CROSS1','MTM_CROSS2','CDLDRAGONFLYDOJI',
                                   'CCI_CROSS2','CDLSPINNINGTOP','CDLHIKKAKEMOD','DMA_CROSS2',
                                   'MIKE_WRJC','CROSS_JC','OSC_CROSS3','RSI_CROSS1','MIKE_WSJC',
                                   'MTM_CROSS3','CDLADVANCEBLOCK','BIAS_CROSS1','CDLCLOSINGMARUBOZU',
                                   'CDL3OUTSIDE','VPT_CROSS3','CDLEVENINGDOJISTAR','CDL2CROWS',
                                   'CDLHANGINGMAN','ADTM_CROSS2','CDLMATCHINGLOW','CDLHIKKAKE',
                                   'CDLKICKING','CDLCOUNTERATTACK','CHO_CROSS1','CDLHARAMI',
                                   'BBI_CROSS1','MIKE_WRSC','CDLINVERTEDHAMMER','CCI_CROSS1',
                                   'CDLBREAKAWAY','CDLGAPSIDESIDEWHITE','DI_CROSS2','CDL3WHITESOLDIERS',
                                   'CDLTRISTAR','CDLXSIDEGAP3METHODS','CDLPIERCING','VPT_CROSS4',
                                   'CDLLONGLINE','CDLDOJI','CDLHIGHWAVE','CDLSTALLEDPATTERN',
                                   'ADX_CROSS1','CDL3LINESTRIKE','CDLBELTHOLD','CDLINNECK',
                                   'CDLONNECK','CDLRICKSHAWMAN','CDLTHRUSTING','CDLIDENTICAL3CROWS',
                                   'SKDJ_CROSS2','CDLDOJISTAR','RSI_CROSS2','OSC_CROSS1',
                                   'CDLRISEFALL3METHODS','CDLLONGLEGGEDDOJI','MIKE_WSSC',
                                   'CDLDARKCLOUDCOVER','CHO_CROSS2','CDLENGULFING']]
                week_data = data[['code','date','date_stamp','AD_WK','ADDI_WK','ADDI_C_WK','ADTM_WK','ADX_WK','ADXR_WK','ADX_C_WK','AD_C_WK',
                                  'AMA_WK','ASI_WK','ASIT_WK','ATR_WK','ATRR_WK','BBI_WK','BIAS1_WK','BIAS2_WK',
                                  'BIAS3_WK','BODY_WK','BODY_ABS_WK','BOLL_WK','CCI_WK','CHO_WK','DDD_WK','DDI_WK',
                                  'DDI_C_WK','DEA_WK','DI1_WK','DI2_WK','DIF_WK','DI_M_WK','KDJ_D_WK','KDJ_J_WK',
                                  'KDJ_K_WK','LB_WK','MA1_WK','MA10_WK','MA120_WK','MA180_WK','MA2_WK','MA20_WK',
                                  'MA3_WK','MA4_WK','MA5_WK','MA60_WK','MAADTM_WK','MACD_WK','MACHO_WK','MAOSC_WK',
                                  'MAVPT_WK','MFI_WK','MFI_C_WK','MIKE_BOLL_WK','MR_WK','MS_WK','MTM_WK','MTMMA_WK',
                                  'OBV_WK','OBV_C_WK','OSC_WK','PRICE_PCG_WK','ROC_WK','ROCMA_WK','RSI1_WK','RSI1_C_WK',
                                  'RSI2_WK','RSI2_C_WK','RSI3_WK','RSI3_C_WK','RSV_WK','SHA_LOW_WK','SHA_UP_WK','SKDJ_D_WK',
                                  'SKDJ_K_WK','SR_WK','SS_WK','TR_WK','UB_WK','VPT_WK','VR_WK','VRSI_WK','VRSI_C_WK','VSTD_WK',
                                  'WIDTH_WK','WR_WK','WR1_WK','WR2_WK','WS_WK','CDLDRAGONFLYDOJI_WK','MIKE_WRJC_WK',
                                  'CDLRICKSHAWMAN_WK','MIKE_WSSC_WK','DI_CROSS2_WK','CDLHARAMI_WK','BBI_CROSS2_WK',
                                  'VPT_CROSS2_WK','CDLBELTHOLD_WK','CDLHAMMER_WK','CDL3INSIDE_WK','CDLTRISTAR_WK',
                                  'OSC_CROSS1_WK','CDLMARUBOZU_WK','CDLTASUKIGAP_WK','CDLSPINNINGTOP_WK','CDLDARKCLOUDCOVER_WK',
                                  'CDL3BLACKCROWS_WK','BIAS_CROSS2_WK','OSC_CROSS3_WK','CHO_CROSS1_WK','CDLMORNINGSTAR_WK',
                                  'ADX_CROSS2_WK','CDLINNECK_WK','ADTM_CROSS2_WK','MACD_TR_WK','CDLDOJI_WK','MTM_CROSS1_WK',
                                  'CDLCOUNTERATTACK_WK','CDLLONGLINE_WK','KDJ_CROSS1_WK','CDLADVANCEBLOCK_WK','CDLHANGINGMAN_WK',
                                  'KDJ_CROSS2_WK','ADX_CROSS1_WK','CDLMATHOLD_WK','CDLABANDONEDBABY_WK','WR_CROSS2_WK',
                                  'MIKE_WRSC_WK','OSC_CROSS2_WK','CDLGAPSIDESIDEWHITE_WK','CROSS_JC_WK','MTM_CROSS4_WK',
                                  'CDLSHOOTINGSTAR_WK','ADTM_CROSS1_WK','CDL3OUTSIDE_WK','CDLLONGLEGGEDDOJI_WK','CDL3LINESTRIKE_WK',
                                  'CDLHIKKAKE_WK','CDLSTALLEDPATTERN_WK','MTM_CROSS2_WK','SKDJ_CROSS2_WK','CDLEVENINGDOJISTAR_WK',
                                  'OSC_CROSS4_WK','CDLTAKURI_WK','CDLSHORTLINE_WK','CROSS_SC_WK','CDLMATCHINGLOW_WK',
                                  'CCI_CROSS4_WK','MIKE_WSJC_WK','CDLHOMINGPIGEON_WK','VPT_CROSS1_WK','CDLCLOSINGMARUBOZU_WK',
                                  'WR_CROSS1_WK','CDLTHRUSTING_WK','BBI_CROSS1_WK','DMA_CROSS2_WK','RSI_CROSS1_WK',
                                  'CDLRISEFALL3METHODS_WK','CDLHIKKAKEMOD_WK','CCI_CROSS3_WK','CDLKICKINGBYLENGTH_WK',
                                  'CDLLADDERBOTTOM_WK','DI_CROSS1_WK','VPT_CROSS3_WK','CDLHARAMICROSS_WK','CHO_CROSS2_WK',
                                  'CCI_CROSS2_WK','CDL3STARSINSOUTH_WK','CDLXSIDEGAP3METHODS_WK','RSI_CROSS2_WK',
                                  'MIKE_TR_WK','CDLDOJISTAR_WK','CDLCONCEALBABYSWALL_WK','CDLPIERCING_WK','CDLHIGHWAVE_WK',
                                  'CDLMORNINGDOJISTAR_WK','CDLSTICKSANDWICH_WK','CDLGRAVESTONEDOJI_WK','CDLINVERTEDHAMMER_WK',
                                  'CDLKICKING_WK','CDLSEPARATINGLINES_WK','CDLBREAKAWAY_WK','MTM_CROSS3_WK','CDLUNIQUE3RIVER_WK',
                                  'CCI_CROSS1_WK','DMA_CROSS1_WK','VPT_CROSS4_WK','CDLEVENINGSTAR_WK','CDL2CROWS_WK',
                                  'CDL3WHITESOLDIERS_WK','CDLIDENTICAL3CROWS_WK','CDLUPSIDEGAP2CROWS_WK','CDLENGULFING_WK',
                                  'SKDJ_CROSS1_WK','BIAS_CROSS1_WK','CDLONNECK_WK']]
                QA_util_log_info(
                    '##JOB01 Pre Data index quant data ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                alpha_res = QA_util_to_json_from_pandas(alpha_data)
                index_res = QA_util_to_json_from_pandas(index_data)
                week_res = QA_util_to_json_from_pandas(week_data)
                QA_util_log_info(
                    '##JOB02 Got Data index quant data ============== {deal_date}'.format(deal_date=deal_date), ui_log)

                try:
                    alpha.insert_many(alpha_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now index quant data alpha saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        alpha.insert_many(alpha_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass

                try:
                    week.insert_many(week_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now index quant data week saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        week.insert_many(week_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass

                try:
                    index.insert_many(index_res, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now index quant data index saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        index.insert_many(index_res, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
            else:
                QA_util_log_info(
                    '##JOB01 No Data index_quant_datat ============== {deal_date} '.format(deal_date=deal_date), ui_log)