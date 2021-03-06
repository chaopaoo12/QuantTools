import cx_Oracle
import pandas as pd
import numpy as np
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code"
,datetime as "datetime"
,VR as VR_15M,
VRSI as VRSI_15M,
VRSI_C as VRSI_C_15M,
VSTD as VSTD_15M,
BOLL as BOLL_15M,
UB as UB_15M,
LB as LB_15M,
WIDTH as WIDTH_15M,
WR as WR_15M,
MR as MR_15M,
SR as SR_15M,
WS as WS_15M,
MS as MS_15M,
SS as SS_15M,
MIKE_WRSC as MIKE_WRSC_15M,
MIKE_WRJC as MIKE_WRJC_15M,
MIKE_WSSC as MIKE_WSSC_15M,
MIKE_WSJC as MIKE_WSJC_15M,
MIKE_TR as MIKE_TR_15M,
MIKE_BOLL as MIKE_BOLL_15M,
ASI as ASI_15M,
ASIT as ASIT_15M,
OBV as OBV_15M,
OBV_C as OBV_C_15M,
VPT as VPT_15M,
MAVPT as MAVPT_15M,
VPT_CROSS1 as VPT_CROSS1_15M,
VPT_CROSS2 as VPT_CROSS2_15M,
VPT_CROSS3 as VPT_CROSS3_15M,
VPT_CROSS4 as VPT_CROSS4_15M,
KDJ_K as KDJ_K_15M,
KDJ_D as KDJ_D_15M,
KDJ_J as KDJ_J_15M,
KDJ_CROSS1 as KDJ_CROSS1_15M,
KDJ_CROSS2 as KDJ_CROSS2_15M,
WR1 as WR1_15M,
WR2 as WR2_15M,
WR_CROSS1 as WR_CROSS1_15M,
WR_CROSS2 as WR_CROSS2_15M,
ROC as ROC_15M,
ROCMA as ROCMA_15M,
RSI1 as RSI1_15M,
RSI2 as RSI2_15M,
RSI3 as RSI3_15M,
RSI1_C as RSI1_C_15M,
RSI2_C as RSI2_C_15M,
RSI3_C as RSI3_C_15M,
RSI_CROSS1 as RSI_CROSS1_15M,
RSI_CROSS2 as RSI_CROSS2_15M,
CCI as CCI_15M,
CCI_CROSS1 as CCI_CROSS1_15M,
CCI_CROSS2 as CCI_CROSS2_15M,
CCI_CROSS3 as CCI_CROSS3_15M,
CCI_CROSS4 as CCI_CROSS4_15M,
BIAS1 as BIAS1_15M,
BIAS2 as BIAS2_15M,
BIAS3 as BIAS3_15M,
BIAS_CROSS1 as BIAS_CROSS1_15M,
BIAS_CROSS2 as BIAS_CROSS2_15M,
OSC as OSC_15M,
MAOSC as MAOSC_15M,
OSC_CROSS1 as OSC_CROSS1_15M,
OSC_CROSS2 as OSC_CROSS2_15M,
OSC_CROSS3 as OSC_CROSS3_15M,
OSC_CROSS4 as OSC_CROSS4_15M,
ADTM as ADTM_15M,
MAADTM as MAADTM_15M,
ADTM_CROSS1 as ADTM_CROSS1_15M,
ADTM_CROSS2 as ADTM_CROSS2_15M,
DIF as DIF_15M,
DEA as DEA_15M,
MACD as MACD_15M,
CROSS_JC as CROSS_JC_15M,
CROSS_SC as CROSS_SC_15M,
MACD_TR as MACD_TR_15M,
DI1 as DI1_15M,
DI2 as DI2_15M,
ADX as ADX_15M,
ADXR as ADXR_15M,
ADX_C as ADX_C_15M,
DI_M as DI_M_15M,
DI_CROSS1 as DI_CROSS1_15M,
DI_CROSS2 as DI_CROSS2_15M,
ADX_CROSS1 as ADX_CROSS1_15M,
ADX_CROSS2 as ADX_CROSS2_15M,
DDD as DDD_15M,
AMA as AMA_15M,
DMA_CROSS1 as DMA_CROSS1_15M,
DMA_CROSS2 as DMA_CROSS2_15M,
MTM as MTM_15M,
MTMMA as MTMMA_15M,
MTM_CROSS1 as MTM_CROSS1_15M,
MTM_CROSS2 as MTM_CROSS2_15M,
MTM_CROSS3 as MTM_CROSS3_15M,
MTM_CROSS4 as MTM_CROSS4_15M,
BBI as BBI_15M,
BBI_CROSS1 as BBI_CROSS1_15M,
BBI_CROSS2 as BBI_CROSS2_15M,
MFI as MFI_15M,
MFI_C as MFI_C_15M,
TR as TR_15M,
ATR as ATR_15M,
ATRR as ATRR_15M,
RSV as RSV_15M,
SKDJ_K as SKDJ_K_15M,
SKDJ_D as SKDJ_D_15M,
SKDJ_CROSS1 as SKDJ_CROSS1_15M,
SKDJ_CROSS2 as SKDJ_CROSS2_15M,
DDI as DDI_15M,
ADDI as ADDI_15M,
AD as AD_15M,
DDI_C as DDI_C_15M,
AD_C as AD_C_15M,
ADDI_C as ADDI_C_15M,
SHA_LOW as SHA_LOW_15M,
SHA_UP as SHA_UP_15M,
BODY as BODY_15M,
BODY_ABS as BODY_ABS_15M,
PRICE_PCG as PRICE_PCG_15M,
MA3 as MA3_15M,
MA5 as MA5_15M,
MA8 as MA8_15M,
MA10 as MA10_15M,
MA12 as MA12_15M,
MA15 as MA15_15M,
MA20 as MA20_15M,
MA30 as MA30_15M,
MA35 as MA35_15M,
MA40 as MA40_15M,
MA45 as MA45_15M,
MA50 as MA50_15M,
MA60 as MA60_15M,
SHORT10 as SHORT10_15M,
SHORT20 as SHORT20_15M,
SHORT60 as SHORT60_15M,
LONG60 as LONG60_15M,
SHORT_CROSS1 as SHORT_CROSS1_15M,
SHORT_CROSS2 as SHORT_CROSS2_15M,
LONG_CROSS1 as LONG_CROSS1_15M,
LONG_CROSS2 as LONG_CROSS2_15M,
LONG_AMOUNT as LONG_AMOUNT_15M,
SHORT_AMOUNT as SHORT_AMOUNT_15M,
GMMA3 as GMMA3_15M,
GMMA5 as GMMA5_15M,
GMMA8 as GMMA8_15M,
GMMA10 as GMMA10_15M,
GMMA12 as GMMA12_15M,
GMMA30 as GMMA30_15M,
GMMA35 as GMMA35_15M,
GMMA40 as GMMA40_15M,
GMMA45 as GMMA45_15M,
GMMA50 as GMMA50_15M,
MA_VOL3 as MA_VOL3_15M,
MA_VOL5 as MA_VOL5_15M,
MA_VOL8 as MA_VOL8_15M,
MA_VOL10 as MA_VOL10_15M,
MA_VOL12 as MA_VOL12_15M,
MA_VOL15 as MA_VOL15_15M,
MA_VOL20 as MA_VOL20_15M,
MA_VOL30 as MA_VOL30_15M,
MA_VOL35 as MA_VOL35_15M,
MA_VOL40 as MA_VOL40_15M,
MA_VOL45 as MA_VOL45_15M,
MA_VOL50 as MA_VOL50_15M,
MA_VOL60 as MA_VOL60_15M,
SHORT10V as SHORT10V_15M,
SHORT20V as SHORT20V_15M,
SHORT60V as SHORT60V_15M,
LONG60V as LONG60V_15M,
SHORTV_CROSS1 as SHORTV_CROSS1_15M,
SHORTV_CROSS2 as SHORTV_CROSS2_15M,
LONGV_CROSS1 as LONGV_CROSS1_15M,
LONGV_CROSS2 as LONGV_CROSS2_15M,
LONGV_AMOUNT as LONGV_AMOUNT_15M,
SHORTV_AMOUNT as SHORTV_AMOUNT_15M,
GMMA_VOL3 as GMMA_VOL3_15M,
GMMA_VOL5 as GMMA_VOL5_15M,
GMMA_VOL8 as GMMA_VOL8_15M,
GMMA_VOL10 as GMMA_VOL10_15M,
GMMA_VOL12 as GMMA_VOL12_15M,
GMMA_VOL30 as GMMA_VOL30_15M,
GMMA_VOL35 as GMMA_VOL35_15M,
GMMA_VOL40 as GMMA_VOL40_15M,
GMMA_VOL45 as GMMA_VOL45_15M,
GMMA_VOL50 as GMMA_VOL50_15M,
CDL2CROWS as CDL2CROWS_15M,
CDL3BLACKCROWS as CDL3BLACKCROWS_15M,
CDL3INSIDE as CDL3INSIDE_15M,
CDL3LINESTRIKE as CDL3LINESTRIKE_15M,
CDL3OUTSIDE as CDL3OUTSIDE_15M,
CDL3STARSINSOUTH as CDL3STARSINSOUTH_15M,
CDL3WHITESOLDIERS as CDL3WHITESOLDIERS_15M,
CDLABANDONEDBABY as CDLABANDONEDBABY_15M,
CDLADVANCEBLOCK as CDLADVANCEBLOCK_15M,
CDLBELTHOLD as CDLBELTHOLD_15M,
CDLBREAKAWAY as CDLBREAKAWAY_15M,
CDLCLOSINGMARUBOZU as CDLCLOSINGMARUBOZU_15M,
CDLCONCEALBABYSWALL as CDLCONCEALBABYSWALL_15M,
CDLCOUNTERATTACK as CDLCOUNTERATTACK_15M,
CDLDARKCLOUDCOVER as CDLDARKCLOUDCOVER_15M,
CDLDOJI as CDLDOJI_15M,
CDLDOJISTAR as CDLDOJISTAR_15M,
CDLDRAGONFLYDOJI as CDLDRAGONFLYDOJI_15M,
CDLENGULFING as CDLENGULFING_15M,
CDLEVENINGDOJISTAR as CDLEVENINGDOJISTAR_15M,
CDLEVENINGSTAR as CDLEVENINGSTAR_15M,
CDLGAPSIDESIDEWHITE as CDLGAPSIDESIDEWHITE_15M,
CDLGRAVESTONEDOJI as CDLGRAVESTONEDOJI_15M,
CDLHAMMER as CDLHAMMER_15M,
CDLHANGINGMAN as CDLHANGINGMAN_15M,
CDLHARAMI as CDLHARAMI_15M,
CDLHARAMICROSS as CDLHARAMICROSS_15M,
CDLHIGHWAVE as CDLHIGHWAVE_15M,
CDLHIKKAKE as CDLHIKKAKE_15M,
CDLHIKKAKEMOD as CDLHIKKAKEMOD_15M,
CDLHOMINGPIGEON as CDLHOMINGPIGEON_15M,
CDLIDENTICAL3CROWS as CDLIDENTICAL3CROWS_15M,
CDLINNECK as CDLINNECK_15M,
CDLINVERTEDHAMMER as CDLINVERTEDHAMMER_15M,
CDLKICKING as CDLKICKING_15M,
CDLKICKINGBYLENGTH as CDLKICKINGBYLENGTH_15M,
CDLLADDERBOTTOM as CDLLADDERBOTTOM_15M,
CDLLONGLEGGEDDOJI as CDLLONGLEGGEDDOJI_15M,
CDLLONGLINE as CDLLONGLINE_15M,
CDLMARUBOZU as CDLMARUBOZU_15M,
CDLMATCHINGLOW as CDLMATCHINGLOW_15M,
CDLMATHOLD as CDLMATHOLD_15M,
CDLMORNINGDOJISTAR as CDLMORNINGDOJISTAR_15M,
CDLMORNINGSTAR as CDLMORNINGSTAR_15M,
CDLONNECK as CDLONNECK_15M,
CDLPIERCING as CDLPIERCING_15M,
CDLRICKSHAWMAN as CDLRICKSHAWMAN_15M,
CDLRISEFALL3METHODS as CDLRISEFALL3METHODS_15M,
CDLSEPARATINGLINES as CDLSEPARATINGLINES_15M,
CDLSHOOTINGSTAR as CDLSHOOTINGSTAR_15M,
CDLSHORTLINE as CDLSHORTLINE_15M,
CDLSPINNINGTOP as CDLSPINNINGTOP_15M,
CDLSTALLEDPATTERN as CDLSTALLEDPATTERN_15M,
CDLSTICKSANDWICH as CDLSTICKSANDWICH_15M,
CDLTAKURI as CDLTAKURI_15M,
CDLTASUKIGAP as CDLTASUKIGAP_15M,
CDLTHRUSTING as CDLTHRUSTING_15M,
CDLTRISTAR as CDLTRISTAR_15M,
CDLUNIQUE3RIVER as CDLUNIQUE3RIVER_15M,
CDLUPSIDEGAP2CROWS as CDLUPSIDEGAP2CROWS_15M,
CDLXSIDEGAP3METHODS as CDLXSIDEGAP3METHODS_15M,
MA5_C as MA5_C_15M,
MA15_C as MA15_C_15M,
MA30_C as MA30_C_15M,
MA60_C as MA60_C_15M,
GMMA3_C as GMMA3_C_15M,
GMMA15_C as GMMA15_C_15M,
GMMA30_C as GMMA30_C_15M,
MA_VOL5_C as MA_VOL5_C_15M,
MA_VOL60_C as MA_VOL60_C_15M,
GMMA_VOL3_C as GMMA_VOL3_C_15M,
GMMA_VOL15_C as GMMA_VOL15_C_15M,
GMMA_VOL30_C as GMMA_VOL30_C_15M,
MA5_D as MA5_D_15M,
MA15_D as MA15_D_15M,
MA30_D as MA30_D_15M,
MA60_D as MA60_D_15M,
GMMA3_D as GMMA3_D_15M,
GMMA_VOL3_D as GMMA_VOL3_D_15M,
GMMA15_D as GMMA15_D_15M,
GMMA_VOL15_D as GMMA_VOL15_D_15M,
GMMA30_D as GMMA30_D_15M,
GMMA_VOL30_D as GMMA_VOL30_D_15M
from INDEX_TECHNICAL_15MIN
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Index_Index15min(from_ , to_, type = 'day', sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Index QuantData Index 15min ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    if type == 'day':
        sql_text = sql_text + " and substr(datetime, 12, 20) = '15:00:00'"
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    if type == 'day':
        data = data.drop_duplicates((['code', 'date'])).set_index(['date','code']).drop('datetime',axis=1)
    else:
        data = data.drop_duplicates((['code', 'datetime']))
        data = data.assign(datetime = data.datetime.apply(lambda x:pd.to_datetime(x))).drop_duplicates((['code', 'datetime'])).set_index(['datetime','code'])
    for columnname in data.columns:
        if data[columnname].dtype == 'object' and columnname not in ['date','datetime','code']:
            data[columnname]=data[columnname].astype('float32')
    data = data.assign(SKDJ_TR_15M = (data.SKDJ_K_15M > data.SKDJ_D_15M) * 1,
                       SHORT_TR_15M = (data.SHORT20_15M > 0)*1,
                       LONG_TR_15M = (data.LONG60_15M > 0)*1)
    return(data)