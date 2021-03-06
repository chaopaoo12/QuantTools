import cx_Oracle
import pandas as pd
import numpy as np
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code"
,VR as VR_WK,
VRSI as VRSI_WK,
VRSI_C as VRSI_C_WK,
VSTD as VSTD_WK,
BOLL as BOLL_WK,
UB as UB_WK,
LB as LB_WK,
WIDTH as WIDTH_WK,
WR as WR_WK,
MR as MR_WK,
SR as SR_WK,
WS as WS_WK,
MS as MS_WK,
SS as SS_WK,
MIKE_WRSC as MIKE_WRSC_WK,
MIKE_WRJC as MIKE_WRJC_WK,
MIKE_WSSC as MIKE_WSSC_WK,
MIKE_WSJC as MIKE_WSJC_WK,
MIKE_TR as MIKE_TR_WK,
MIKE_BOLL as MIKE_BOLL_WK,
ASI as ASI_WK,
ASIT as ASIT_WK,
OBV as OBV_WK,
OBV_C as OBV_C_WK,
VPT as VPT_WK,
MAVPT as MAVPT_WK,
VPT_CROSS1 as VPT_CROSS1_WK,
VPT_CROSS2 as VPT_CROSS2_WK,
VPT_CROSS3 as VPT_CROSS3_WK,
VPT_CROSS4 as VPT_CROSS4_WK,
KDJ_K as KDJ_K_WK,
KDJ_D as KDJ_D_WK,
KDJ_J as KDJ_J_WK,
KDJ_CROSS1 as KDJ_CROSS1_WK,
KDJ_CROSS2 as KDJ_CROSS2_WK,
WR1 as WR1_WK,
WR2 as WR2_WK,
WR_CROSS1 as WR_CROSS1_WK,
WR_CROSS2 as WR_CROSS2_WK,
ROC as ROC_WK,
ROCMA as ROCMA_WK,
RSI1 as RSI1_WK,
RSI2 as RSI2_WK,
RSI3 as RSI3_WK,
RSI1_C as RSI1_C_WK,
RSI2_C as RSI2_C_WK,
RSI3_C as RSI3_C_WK,
RSI_CROSS1 as RSI_CROSS1_WK,
RSI_CROSS2 as RSI_CROSS2_WK,
CCI as CCI_WK,
CCI_CROSS1 as CCI_CROSS1_WK,
CCI_CROSS2 as CCI_CROSS2_WK,
CCI_CROSS3 as CCI_CROSS3_WK,
CCI_CROSS4 as CCI_CROSS4_WK,
BIAS1 as BIAS1_WK,
BIAS2 as BIAS2_WK,
BIAS3 as BIAS3_WK,
BIAS_CROSS1 as BIAS_CROSS1_WK,
BIAS_CROSS2 as BIAS_CROSS2_WK,
OSC as OSC_WK,
MAOSC as MAOSC_WK,
OSC_CROSS1 as OSC_CROSS1_WK,
OSC_CROSS2 as OSC_CROSS2_WK,
OSC_CROSS3 as OSC_CROSS3_WK,
OSC_CROSS4 as OSC_CROSS4_WK,
ADTM as ADTM_WK,
MAADTM as MAADTM_WK,
ADTM_CROSS1 as ADTM_CROSS1_WK,
ADTM_CROSS2 as ADTM_CROSS2_WK,
DIF as DIF_WK,
DEA as DEA_WK,
MACD as MACD_WK,
CROSS_JC as CROSS_JC_WK,
CROSS_SC as CROSS_SC_WK,
MACD_TR as MACD_TR_WK,
DI1 as DI1_WK,
DI2 as DI2_WK,
ADX as ADX_WK,
ADXR as ADXR_WK,
ADX_C as ADX_C_WK,
DI_M as DI_M_WK,
DI_CROSS1 as DI_CROSS1_WK,
DI_CROSS2 as DI_CROSS2_WK,
ADX_CROSS1 as ADX_CROSS1_WK,
ADX_CROSS2 as ADX_CROSS2_WK,
DDD as DDD_WK,
AMA as AMA_WK,
DMA_CROSS1 as DMA_CROSS1_WK,
DMA_CROSS2 as DMA_CROSS2_WK,
MTM as MTM_WK,
MTMMA as MTMMA_WK,
MTM_CROSS1 as MTM_CROSS1_WK,
MTM_CROSS2 as MTM_CROSS2_WK,
MTM_CROSS3 as MTM_CROSS3_WK,
MTM_CROSS4 as MTM_CROSS4_WK,
BBI as BBI_WK,
BBI_CROSS1 as BBI_CROSS1_WK,
BBI_CROSS2 as BBI_CROSS2_WK,
MFI as MFI_WK,
MFI_C as MFI_C_WK,
TR as TR_WK,
ATR as ATR_WK,
ATRR as ATRR_WK,
RSV as RSV_WK,
SKDJ_K as SKDJ_K_WK,
SKDJ_D as SKDJ_D_WK,
SKDJ_CROSS1 as SKDJ_CROSS1_WK,
SKDJ_CROSS2 as SKDJ_CROSS2_WK,
DDI as DDI_WK,
ADDI as ADDI_WK,
AD as AD_WK,
DDI_C as DDI_C_WK,
AD_C as AD_C_WK,
ADDI_C as ADDI_C_WK,
SHA_LOW as SHA_LOW_WK,
SHA_UP as SHA_UP_WK,
BODY as BODY_WK,
BODY_ABS as BODY_ABS_WK,
PRICE_PCG as PRICE_PCG_WK,
MA3 as MA3_WK,
MA5 as MA5_WK,
MA8 as MA8_WK,
MA10 as MA10_WK,
MA12 as MA12_WK,
MA15 as MA15_WK,
MA20 as MA20_WK,
MA30 as MA30_WK,
MA35 as MA35_WK,
MA40 as MA40_WK,
MA45 as MA45_WK,
MA50 as MA50_WK,
MA60 as MA60_WK,
SHORT10 as SHORT10_WK,
SHORT20 as SHORT20_WK,
SHORT60 as SHORT60_WK,
LONG60 as LONG60_WK,
SHORT_CROSS1 as SHORT_CROSS1_WK,
SHORT_CROSS2 as SHORT_CROSS2_WK,
LONG_CROSS1 as LONG_CROSS1_WK,
LONG_CROSS2 as LONG_CROSS2_WK,
LONG_AMOUNT as LONG_AMOUNT_WK,
SHORT_AMOUNT as SHORT_AMOUNT_WK,
GMMA3 as GMMA3_WK,
GMMA5 as GMMA5_WK,
GMMA8 as GMMA8_WK,
GMMA10 as GMMA10_WK,
GMMA12 as GMMA12_WK,
GMMA30 as GMMA30_WK,
GMMA35 as GMMA35_WK,
GMMA40 as GMMA40_WK,
GMMA45 as GMMA45_WK,
GMMA50 as GMMA50_WK,
MA_VOL3 as MA_VOL3_WK,
MA_VOL5 as MA_VOL5_WK,
MA_VOL8 as MA_VOL8_WK,
MA_VOL10 as MA_VOL10_WK,
MA_VOL12 as MA_VOL12_WK,
MA_VOL15 as MA_VOL15_WK,
MA_VOL20 as MA_VOL20_WK,
MA_VOL30 as MA_VOL30_WK,
MA_VOL35 as MA_VOL35_WK,
MA_VOL40 as MA_VOL40_WK,
MA_VOL45 as MA_VOL45_WK,
MA_VOL50 as MA_VOL50_WK,
MA_VOL60 as MA_VOL60_WK,
SHORT10V as SHORT10V_WK,
SHORT20V as SHORT20V_WK,
SHORT60V as SHORT60V_WK,
LONG60V as LONG60V_WK,
SHORTV_CROSS1 as SHORTV_CROSS1_WK,
SHORTV_CROSS2 as SHORTV_CROSS2_WK,
LONGV_CROSS1 as LONGV_CROSS1_WK,
LONGV_CROSS2 as LONGV_CROSS2_WK,
LONGV_AMOUNT as LONGV_AMOUNT_WK,
SHORTV_AMOUNT as SHORTV_AMOUNT_WK,
GMMA_VOL3 as GMMA_VOL3_WK,
GMMA_VOL5 as GMMA_VOL5_WK,
GMMA_VOL8 as GMMA_VOL8_WK,
GMMA_VOL10 as GMMA_VOL10_WK,
GMMA_VOL12 as GMMA_VOL12_WK,
GMMA_VOL30 as GMMA_VOL30_WK,
GMMA_VOL35 as GMMA_VOL35_WK,
GMMA_VOL40 as GMMA_VOL40_WK,
GMMA_VOL45 as GMMA_VOL45_WK,
GMMA_VOL50 as GMMA_VOL50_WK,
CDL2CROWS as CDL2CROWS_WK,
CDL3BLACKCROWS as CDL3BLACKCROWS_WK,
CDL3INSIDE as CDL3INSIDE_WK,
CDL3LINESTRIKE as CDL3LINESTRIKE_WK,
CDL3OUTSIDE as CDL3OUTSIDE_WK,
CDL3STARSINSOUTH as CDL3STARSINSOUTH_WK,
CDL3WHITESOLDIERS as CDL3WHITESOLDIERS_WK,
CDLABANDONEDBABY as CDLABANDONEDBABY_WK,
CDLADVANCEBLOCK as CDLADVANCEBLOCK_WK,
CDLBELTHOLD as CDLBELTHOLD_WK,
CDLBREAKAWAY as CDLBREAKAWAY_WK,
CDLCLOSINGMARUBOZU as CDLCLOSINGMARUBOZU_WK,
CDLCONCEALBABYSWALL as CDLCONCEALBABYSWALL_WK,
CDLCOUNTERATTACK as CDLCOUNTERATTACK_WK,
CDLDARKCLOUDCOVER as CDLDARKCLOUDCOVER_WK,
CDLDOJI as CDLDOJI_WK,
CDLDOJISTAR as CDLDOJISTAR_WK,
CDLDRAGONFLYDOJI as CDLDRAGONFLYDOJI_WK,
CDLENGULFING as CDLENGULFING_WK,
CDLEVENINGDOJISTAR as CDLEVENINGDOJISTAR_WK,
CDLEVENINGSTAR as CDLEVENINGSTAR_WK,
CDLGAPSIDESIDEWHITE as CDLGAPSIDESIDEWHITE_WK,
CDLGRAVESTONEDOJI as CDLGRAVESTONEDOJI_WK,
CDLHAMMER as CDLHAMMER_WK,
CDLHANGINGMAN as CDLHANGINGMAN_WK,
CDLHARAMI as CDLHARAMI_WK,
CDLHARAMICROSS as CDLHARAMICROSS_WK,
CDLHIGHWAVE as CDLHIGHWAVE_WK,
CDLHIKKAKE as CDLHIKKAKE_WK,
CDLHIKKAKEMOD as CDLHIKKAKEMOD_WK,
CDLHOMINGPIGEON as CDLHOMINGPIGEON_WK,
CDLIDENTICAL3CROWS as CDLIDENTICAL3CROWS_WK,
CDLINNECK as CDLINNECK_WK,
CDLINVERTEDHAMMER as CDLINVERTEDHAMMER_WK,
CDLKICKING as CDLKICKING_WK,
CDLKICKINGBYLENGTH as CDLKICKINGBYLENGTH_WK,
CDLLADDERBOTTOM as CDLLADDERBOTTOM_WK,
CDLLONGLEGGEDDOJI as CDLLONGLEGGEDDOJI_WK,
CDLLONGLINE as CDLLONGLINE_WK,
CDLMARUBOZU as CDLMARUBOZU_WK,
CDLMATCHINGLOW as CDLMATCHINGLOW_WK,
CDLMATHOLD as CDLMATHOLD_WK,
CDLMORNINGDOJISTAR as CDLMORNINGDOJISTAR_WK,
CDLMORNINGSTAR as CDLMORNINGSTAR_WK,
CDLONNECK as CDLONNECK_WK,
CDLPIERCING as CDLPIERCING_WK,
CDLRICKSHAWMAN as CDLRICKSHAWMAN_WK,
CDLRISEFALL3METHODS as CDLRISEFALL3METHODS_WK,
CDLSEPARATINGLINES as CDLSEPARATINGLINES_WK,
CDLSHOOTINGSTAR as CDLSHOOTINGSTAR_WK,
CDLSHORTLINE as CDLSHORTLINE_WK,
CDLSPINNINGTOP as CDLSPINNINGTOP_WK,
CDLSTALLEDPATTERN as CDLSTALLEDPATTERN_WK,
CDLSTICKSANDWICH as CDLSTICKSANDWICH_WK,
CDLTAKURI as CDLTAKURI_WK,
CDLTASUKIGAP as CDLTASUKIGAP_WK,
CDLTHRUSTING as CDLTHRUSTING_WK,
CDLTRISTAR as CDLTRISTAR_WK,
CDLUNIQUE3RIVER as CDLUNIQUE3RIVER_WK,
CDLUPSIDEGAP2CROWS as CDLUPSIDEGAP2CROWS_WK,
CDLXSIDEGAP3METHODS as CDLXSIDEGAP3METHODS_WK,
MA5_C as MA5_C_WK,
MA15_C as MA15_C_WK,
MA30_C as MA30_C_WK,
MA60_C as MA60_C_WK,
GMMA3_C as GMMA3_C_WK,
GMMA15_C as GMMA15_C_WK,
GMMA30_C as GMMA30_C_WK,
MA_VOL5_C as MA_VOL5_C_WK,
MA_VOL60_C as MA_VOL60_C_WK,
GMMA_VOL3_C as GMMA_VOL3_C_WK,
GMMA_VOL15_C as GMMA_VOL15_C_WK,
GMMA_VOL30_C as GMMA_VOL30_C_WK,
MA5_D as MA5_D_WK,
MA15_D as MA15_D_WK,
MA30_D as MA30_D_WK,
MA60_D as MA60_D_WK,
GMMA3_D as GMMA3_D_WK,
GMMA_VOL3_D as GMMA_VOL3_D_WK,
GMMA15_D as GMMA15_D_WK,
GMMA_VOL15_D as GMMA_VOL15_D_WK,
GMMA30_D as GMMA30_D_WK,
GMMA_VOL30_D as GMMA_VOL30_D_WK
from INDEX_TECHNICAL_WEEK
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Index_IndexWeek(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Index QuantData Index Week ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    data = data.drop_duplicates((['code', 'date'])).set_index(['date','code'])
    for columnname in data.columns:
        if data[columnname].dtype == 'object':
            data[columnname]=data[columnname].astype('float32')
    data = data.assign(SKDJ_TR_WK = (data.SKDJ_K_WK > data.SKDJ_D_WK)*1,
                       SHORT_WK = (data.SHORT20_WK > 0)*1,
                       LONG_WK = (data.LONG60_WK > 0)*1)
    return(data)