import cx_Oracle
import pandas as pd
import numpy as np
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code"
,datetime
,VR as VR_HR
,VRSI as VRSI_HR
,VRSI_C as VRSI_C_HR
,VSTD as VSTD_HR
,BOLL as BOLL_HR
,UB as UB_HR
,LB as LB_HR
,WIDTH as WIDTH_HR
,WR as WR_HR
,MR as MR_HR
,SR as SR_HR
,WS as WS_HR
,MS as MS_HR
,SS as SS_HR
,MIKE_WRSC as MIKE_WRSC_HR
,MIKE_WRJC as MIKE_WRJC_HR
,MIKE_WSSC as MIKE_WSSC_HR
,MIKE_WSJC as MIKE_WSJC_HR
,MIKE_TR as MIKE_TR_HR
,MIKE_BOLL as MIKE_BOLL_HR
,ASI as ASI_HR
,ASIT as ASIT_HR
,OBV as OBV_HR
,OBV_C as OBV_C_HR
,PVT as PVT_HR
,VPT as VPT_HR
,MAVPT as MAVPT_HR
,VPT_CROSS1 as VPT_CROSS1_HR
,VPT_CROSS2 as VPT_CROSS2_HR
,VPT_CROSS3 as VPT_CROSS3_HR
,VPT_CROSS4 as VPT_CROSS4_HR
,KDJ_K as KDJ_K_HR
,KDJ_D as KDJ_D_HR
,KDJ_J as KDJ_J_HR
,KDJ_CROSS1 as KDJ_CROSS1_HR
,KDJ_CROSS2 as KDJ_CROSS2_HR
,WR1 as WR1_HR
,WR2 as WR2_HR
,WR_CROSS1 as WR_CROSS1_HR
,WR_CROSS2 as WR_CROSS2_HR
,ROC as ROC_HR
,ROCMA as ROCMA_HR
,RSI1 as RSI1_HR
,RSI2 as RSI2_HR
,RSI3 as RSI3_HR
,RSI1_C as RSI1_C_HR
,RSI2_C as RSI2_C_HR
,RSI3_C as RSI3_C_HR
,RSI_CROSS1 as RSI_CROSS1_HR
,RSI_CROSS2 as RSI_CROSS2_HR
,CCI as CCI_HR
,CCI_CROSS1 as CCI_CROSS1_HR
,CCI_CROSS2 as CCI_CROSS2_HR
,CCI_CROSS3 as CCI_CROSS3_HR
,CCI_CROSS4 as CCI_CROSS4_HR
,BIAS1 as BIAS1_HR
,BIAS2 as BIAS2_HR
,BIAS3 as BIAS3_HR
,BIAS_CROSS1 as BIAS_CROSS1_HR
,BIAS_CROSS2 as BIAS_CROSS2_HR
,OSC as OSC_HR
,MAOSC as MAOSC_HR
,OSC_CROSS1 as OSC_CROSS1_HR
,OSC_CROSS2 as OSC_CROSS2_HR
,OSC_CROSS3 as OSC_CROSS3_HR
,OSC_CROSS4 as OSC_CROSS4_HR
,ADTM as ADTM_HR
,MAADTM as MAADTM_HR
,ADTM_CROSS1 as ADTM_CROSS1_HR
,ADTM_CROSS2 as ADTM_CROSS2_HR
,DIF as DIF_HR
,DEA as DEA_HR
,MACD as MACD_HR
,CROSS_JC as CROSS_JC_HR
,CROSS_SC as CROSS_SC_HR
,MACD_TR as MACD_TR_HR
,DI1 as DI1_HR
,DI2 as DI2_HR
,ADX as ADX_HR
,ADXR as ADXR_HR
,ADX_C as ADX_C_HR
,DI_M as DI_M_HR
,DI_CROSS1 as DI_CROSS1_HR
,DI_CROSS2 as DI_CROSS2_HR
,ADX_CROSS1 as ADX_CROSS1_HR
,ADX_CROSS2 as ADX_CROSS2_HR
,DDD as DDD_HR
,AMA as AMA_HR
,DMA_CROSS1 as DMA_CROSS1_HR
,DMA_CROSS2 as DMA_CROSS2_HR
,MTM as MTM_HR
,MTMMA as MTMMA_HR
,MTM_CROSS1 as MTM_CROSS1_HR
,MTM_CROSS2 as MTM_CROSS2_HR
,MTM_CROSS3 as MTM_CROSS3_HR
,MTM_CROSS4 as MTM_CROSS4_HR
,MA1 as MA1_HR
,MA2 as MA2_HR
,MA3 as MA3_HR
,MA4 as MA4_HR
,CHO as CHO_HR
,MACHO as MACHO_HR
,CHO_CROSS1 as CHO_CROSS1_HR
,CHO_CROSS2 as CHO_CROSS2_HR
,BBI as BBI_HR
,BBI_CROSS1 as BBI_CROSS1_HR
,BBI_CROSS2 as BBI_CROSS2_HR
,MFI as MFI_HR
,MFI_C as MFI_C_HR
,TR as TR_HR
,ATR as ATR_HR
,ATRR as ATRR_HR
,RSV as RSV_HR
,SKDJ_K as SKDJ_K_HR
,SKDJ_D as SKDJ_D_HR
,SKDJ_CROSS1 as SKDJ_CROSS1_HR
,SKDJ_CROSS2 as SKDJ_CROSS2_HR
,DDI as DDI_HR
,ADDI as ADDI_HR
,AD as AD_HR
,DDI_C as DDI_C_HR
,AD_C as AD_C_HR
,ADDI_C as ADDI_C_HR
,SHA_LOW as SHA_LOW_HR
,SHA_UP as SHA_UP_HR
,BODY as BODY_HR
,BODY_ABS as BODY_ABS_HR
,PRICE_PCG as PRICE_PCG_HR
,MA5 as MA5_HR
,MA10 as MA10_HR
,MA20 as MA20_HR
,MA60 as MA60_HR
,MA120 as MA120_HR
,MA180 as MA180_HR
,SHORT10 as SHORT10_HR
,SHORT20 as SHORT20_HR
,SHORT60 as SHORT60_HR
,LONG60 as LONG60_HR
,LONG120 as LONG120_HR
,LONG180 as LONG180_HR
,SHORT_CROSS1 as SHORT_CROSS1_HR
,SHORT_CROSS2 as SHORT_CROSS2_HR
,LONG_CROSS1 as LONG_CROSS1_HR
,LONG_CROSS2 as LONG_CROSS2_HR
,LONG_AMOUNT as LONG_AMOUNT_HR
,SHORT_AMOUNT as SHORT_AMOUNT_HR
,CDL2CROWS as CDL2CROWS_HR
,CDL3BLACKCROWS as CDL3BLACKCROWS_HR
,CDL3INSIDE as CDL3INSIDE_HR
,CDL3LINESTRIKE as CDL3LINESTRIKE_HR
,CDL3OUTSIDE as CDL3OUTSIDE_HR
,CDL3STARSINSOUTH as CDL3STARSINSOUTH_HR
,CDL3WHITESOLDIERS as CDL3WHITESOLDIERS_HR
,CDLABANDONEDBABY as CDLABANDONEDBABY_HR
,CDLADVANCEBLOCK as CDLADVANCEBLOCK_HR
,CDLBELTHOLD as CDLBELTHOLD_HR
,CDLBREAKAWAY as CDLBREAKAWAY_HR
,CDLCLOSINGMARUBOZU as CDLCLOSINGMARUBOZU_HR
,CDLCONCEALBABYSWALL as CDLCONCEALBABYSWALL_HR
,CDLCOUNTERATTACK as CDLCOUNTERATTACK_HR
,CDLDARKCLOUDCOVER as CDLDARKCLOUDCOVER_HR
,CDLDOJI as CDLDOJI_HR
,CDLDOJISTAR as CDLDOJISTAR_HR
,CDLDRAGONFLYDOJI as CDLDRAGONFLYDOJI_HR
,CDLENGULFING as CDLENGULFING_HR
,CDLEVENINGDOJISTAR as CDLEVENINGDOJISTAR_HR
,CDLEVENINGSTAR as CDLEVENINGSTAR_HR
,CDLGAPSIDESIDEWHITE as CDLGAPSIDESIDEWHITE_HR
,CDLGRAVESTONEDOJI as CDLGRAVESTONEDOJI_HR
,CDLHAMMER as CDLHAMMER_HR
,CDLHANGINGMAN as CDLHANGINGMAN_HR
,CDLHARAMI as CDLHARAMI_HR
,CDLHARAMICROSS as CDLHARAMICROSS_HR
,CDLHIGHWAVE as CDLHIGHWAVE_HR
,CDLHIKKAKE as CDLHIKKAKE_HR
,CDLHIKKAKEMOD as CDLHIKKAKEMOD_HR
,CDLHOMINGPIGEON as CDLHOMINGPIGEON_HR
,CDLIDENTICAL3CROWS as CDLIDENTICAL3CROWS_HR
,CDLINNECK as CDLINNECK_HR
,CDLINVERTEDHAMMER as CDLINVERTEDHAMMER_HR
,CDLKICKING as CDLKICKING_HR
,CDLKICKINGBYLENGTH as CDLKICKINGBYLENGTH_HR
,CDLLADDERBOTTOM as CDLLADDERBOTTOM_HR
,CDLLONGLEGGEDDOJI as CDLLONGLEGGEDDOJI_HR
,CDLLONGLINE as CDLLONGLINE_HR
,CDLMARUBOZU as CDLMARUBOZU_HR
,CDLMATCHINGLOW as CDLMATCHINGLOW_HR
,CDLMATHOLD as CDLMATHOLD_HR
,CDLMORNINGDOJISTAR as CDLMORNINGDOJISTAR_HR
,CDLMORNINGSTAR as CDLMORNINGSTAR_HR
,CDLONNECK as CDLONNECK_HR
,CDLPIERCING as CDLPIERCING_HR
,CDLRICKSHAWMAN as CDLRICKSHAWMAN_HR
,CDLRISEFALL3METHODS as CDLRISEFALL3METHODS_HR
,CDLSEPARATINGLINES as CDLSEPARATINGLINES_HR
,CDLSHOOTINGSTAR as CDLSHOOTINGSTAR_HR
,CDLSHORTLINE as CDLSHORTLINE_HR
,CDLSPINNINGTOP as CDLSPINNINGTOP_HR
,CDLSTALLEDPATTERN as CDLSTALLEDPATTERN_HR
,CDLSTICKSANDWICH as CDLSTICKSANDWICH_HR
,CDLTAKURI as CDLTAKURI_HR
,CDLTASUKIGAP as CDLTASUKIGAP_HR
,CDLTHRUSTING as CDLTHRUSTING_HR
,CDLTRISTAR as CDLTRISTAR_HR
,CDLUNIQUE3RIVER as CDLUNIQUE3RIVER_HR
,CDLUPSIDEGAP2CROWS as CDLUPSIDEGAP2CROWS_HR
,CDLXSIDEGAP3METHODS as CDLXSIDEGAP3METHODS_HR
,MA5_C as MA5_C_HR
,MA10_C as MA10_C_HR
,MA20_C as MA20_C_HR
,MA60_C as MA60_C_HR
,MA120_C as MA120_C_HR
,MA180_C as MA180_C_HR
from INDEX_TECHNICAL_HOUR
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
and substr(datetime, 12, 20) = '15:00:00'
'''

def QA_Sql_Index_IndexHour(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Index QuantData Index Hour ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    data = data.drop_duplicates((['code', 'datetime'])).set_index(['datetime','code'])
    data['CCI_JC_HR'] = data['CCI_CROSS1_HR'] + data['CCI_CROSS3_HR']
    data['CCI_SC_HR'] = data['CCI_CROSS2_HR'] + data['CCI_CROSS4_HR']
    data.loc[data.CCI_JC_HR==1,'CCI_JC_HR'] = 2
    data.loc[data.CCI_SC_HR==2,'CCI_SC_HR'] = 1
    data['CCI_TR_HR'] = data['CCI_JC_HR'] + data['CCI_SC_HR']
    data.loc[(data.CCI_TR_HR == 0),'CCI_TR_HR'] = np.nan
    data[['CCI_CROSS1_HR','CCI_CROSS2_HR','CCI_CROSS3_HR','CCI_CROSS4_HR','CCI_JC_HR','CCI_SC_HR','CCI_TR_HR']] = data[['CCI_CROSS1_HR','CCI_CROSS2_HR','CCI_CROSS3_HR','CCI_CROSS4_HR','CCI_JC_HR','CCI_SC_HR','CCI_TR_HR']].groupby('code').fillna(method='ffill')
    data['CCI_TR_HR'] = data['CCI_TR_HR'] -1
    data['TERNS_HR'] = data.apply(lambda x: (x.SHORT20_HR > 0) * (x.LONG60_HR > 0) * (x.LONG_AMOUNT_HR > 0) * 1, axis=1)
    return(data)