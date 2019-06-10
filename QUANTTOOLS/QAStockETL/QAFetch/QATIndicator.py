from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv,QA_fetch_stock_day_adv
import QUANTAXIS as QA
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_indicator(code, start_date, end_date):
    data = QA_fetch_stock_day_adv(code,start_date,end_date)
    if data == None:
        return None
    else:
        data = data.to_qfq()
        try:
            VR = data.add_func(QA.QA_indicator_VR)['VR']
        except:
            VR = pd.DataFrame()
        try:
            VRSI = data.add_func(QA.QA_indicator_VRSI)
            VRSI['VRSI_C'] = VRSI['VRSI']/QA.REF(VRSI['VRSI'], 1)-1
        except:
            VRSI = pd.DataFrame()
        try:
            VSTD = data.add_func(QA.QA_indicator_VSTD)
            VSTD['VSTD'] = data.data['volume']/VSTD['VSTD']-1
            ## todo VOL比较
        except:
            VSTD = pd.DataFrame()
        try:
            BOLL = data.add_func(QA.QA_indicator_BOLL)
            BOLL['WIDTH'] = (BOLL['UB']-BOLL['LB'])/BOLL['BOLL']
            BOLL['BOLL_CROSS1'] = QA.CROSS(BOLL['UB'], BOLL['BOLL'])
            BOLL['BOLL_CROSS2'] = QA.CROSS(BOLL['BOLL'], BOLL['UB'])
            BOLL['BOLL_CROSS3'] = QA.CROSS(BOLL['LB'], BOLL['BOLL'])
            BOLL['BOLL_CROSS4'] = QA.CROSS(BOLL['BOLL'], BOLL['LB'])
        except:
            BOLL = pd.DataFrame()
        try:
            MIKE = data.add_func(QA.QA_indicator_MIKE)
            MIKE['WR'] = MIKE['WR'] - data.data['close']
            MIKE['MR'] = MIKE['MR'] - data.data['close']
            MIKE['SR'] = MIKE['SR'] - data.data['close']
            MIKE['WS'] = MIKE['WS'] - data.data['close']
            MIKE['MS'] = MIKE['MS'] - data.data['close']
            MIKE['SS'] = MIKE['SS'] - data.data['close']
        except:
            MIKE = pd.DataFrame()
        try:
            ASI = data.add_func(QA.QA_indicator_ASI)
        except:
            ASI = pd.DataFrame()
        try:
            OBV = data.add_func(QA.QA_indicator_OBV)
            VR['OBV_C'] = VR['OBV']/QA.REF(VR['OBV'], 1)-1
        except:
            OBV = pd.DataFrame()
        try:
            PVT = data.add_func(QA.QA_indicator_PVT)
            PVT['PVT_C'] = VR['PVT']/QA.REF(PVT['PVT'], 1)-1
        except:
            PVT = pd.DataFrame()
        try:
            VPT = data.add_func(QA.QA_indicator_VPT)
            VPT['MARK'] = 0
            VPT['VPT_CROSS1'] = QA.CROSS(VPT['VPT'], VPT['MARK'])
            VPT['VPT_CROSS2'] = QA.CROSS(VPT['MARK'], VPT['VPT'])
            VPT['VPT_CROSS3'] = QA.CROSS(VPT['MAVPT'], VPT['MARK'])
            VPT['VPT_CROSS4'] = QA.CROSS(VPT['MARK'], VPT['MAVPT'])
        except:
            VPT = pd.DataFrame()
        try:
            KDJ = data.add_func(QA.QA_indicator_KDJ)
            KDJ['KDJ_CROSS1'] = QA.CROSS(KDJ['KDJ_D'], KDJ['KDJ_K'])
            KDJ['KDJ_CROSS2'] = QA.CROSS(KDJ['KDJ_K'], KDJ['KDJ_D'])
        except:
            KDJ = pd.DataFrame()
        try:
            WR = data.add_func(QA.QA_indicator_WR,10,6)
            WR['WR_CROSS1'] = QA.CROSS(WR['WR1'], WR['WR2'])
            WR['WR_CROSS2'] = QA.CROSS(WR['WR2'], WR['WR1'])
        except:
            WR = pd.DataFrame()
        try:
            ROC = data.add_func(QA.QA_indicator_ROC)
        except:
            ROC = pd.DataFrame()
        try:
            RSI = data.add_func(QA.QA_indicator_RSI)
            RSI['RSI1_C'] = RSI['RSI1']/QA.REF(RSI['RSI1'], 1)-1
            RSI['RSI2_C'] = RSI['RSI2']/QA.REF(RSI['RSI2'], 1)-1
            RSI['RSI3_C'] = RSI['RSI3']/QA.REF(RSI['RSI3'], 1)-1
            RSI['RSI_CROSS1'] = QA.CROSS(RSI['RSI1'], RSI['RSI3'])
            RSI['RSI_CROSS2'] = QA.CROSS(RSI['RSI3'], RSI['RSI1'])
        except:
            RSI = pd.DataFrame()
        try:
            CCI = data.add_func(QA.QA_indicator_CCI)['CCI']
            CCI['CCI_CROSS1'] = QA.CROSS(CCI['CCI'], CCI['a'])
            CCI['CCI_CROSS2'] = QA.CROSS(CCI['a'], CCI['CCI'])
            CCI['CCI_CROSS3'] = QA.CROSS(CCI['CCI'], CCI['b'])
            CCI['CCI_CROSS4'] = QA.CROSS(CCI['b'], CCI['CCI'])
        except:
            CCI = pd.DataFrame()
        try:
            BIAS = data.add_func(QA.QA_indicator_BIAS,6,12,24)
            BIAS['BIAS_CROSS1'] = QA.CROSS(BIAS['BIAS1'], BIAS['BIAS3'])
            BIAS['BIAS_CROSS2'] = QA.CROSS(BIAS['BIAS3'], BIAS['BIAS1'])
        except:
            BIAS = pd.DataFrame()
        try:
            OSC = data.add_func(QA.QA_indicator_OSC)
            OSC['MARK'] = 0
            OSC['OSC_CROSS1'] = QA.CROSS(OSC['OSC'], OSC['MARK'])
            OSC['OSC_CROSS2'] = QA.CROSS(OSC['MARK'], OSC['OSC'])
            OSC['OSC_CROSS3'] = QA.CROSS(OSC['MAOSC'], OSC['MARK'])
            OSC['OSC_CROSS4'] = QA.CROSS(OSC['MARK'], OSC['MAOSC'])
        except:
            OSC = pd.DataFrame()
        try:
            ADTM = data.add_func(QA.QA_indicator_ADTM)
        except:
            ADTM = pd.DataFrame()
        try:
            MACD = data.add_func(QA.QA_indicator_MACD)
            MACD['CROSS_JC'] = QA.CROSS(MACD['DIFF'], MACD['DEA'])
            MACD['CROSS_SC'] = QA.CROSS(MACD['DEA'], MACD['DIFF'])
        except:
            MACD = pd.DataFrame()
        try:
            DMI = data.add_func(QA.QA_indicator_DMI)
            DMI['ADX_C'] = DMI['ADX']/QA.REF(DMI['ADX'], 1)-1
            DMI['DI_M'] = DMI['DI1'] - DMI['DI2']
            DMI['DI_CROSS1'] = QA.CROSS(DMI['DI1'], DMI['DI2'])
            DMI['DI_CROSS2'] = QA.CROSS(DMI['DI2'], DMI['DI1'])
            DMI['ADX_CROSS1'] = QA.CROSS(DMI['ADX'], DMI['ADXR'])
            DMI['ADX_CROSS2'] = QA.CROSS(DMI['ADXR'], DMI['ADX'])
        except:
            DMI = pd.DataFrame()
        try:
            DMA = data.add_func(QA.QA_indicator_DMA)
            DMA['DMA_CROSS1'] = QA.CROSS(DMA['AMA'], DMA['DDD'])
            DMA['DMA_CROSS2'] = QA.CROSS(DMA['DDD'], DMA['AMA'])
        except:
            DMA = pd.DataFrame()
        try:
            PBX = data.add_func(QA.QA_indicator_PBX)
            PBX['PBX1_C'] = PBX['PBX1']/QA.REF(PBX['PBX1'], 1)-1
            PBX['PBX2_C'] = PBX['PBX2']/QA.REF(PBX['PBX2'], 1)-1
            PBX['PBX3_C'] = PBX['PBX3']/QA.REF(PBX['PBX3'], 1)-1
            PBX['PBX4_C'] = PBX['PBX4']/QA.REF(PBX['PBX4'], 1)-1
            PBX['PBX5_C'] = PBX['PBX5']/QA.REF(PBX['PBX5'], 1)-1
            PBX['PBX6_C'] = PBX['PBX6']/QA.REF(PBX['PBX6'], 1)-1
        except:
            PBX = pd.DataFrame()
        try:
            MTM = data.add_func(QA.QA_indicator_MTM)
            MTM['MARK'] = 0
            MTM['MTM_CROSS1'] = QA.CROSS(MTM['MTM'], MTM['MARK'])
            MTM['MTM_CROSS2'] = QA.CROSS(MTM['MARK'], MTM['MTM'])
            MTM['MTM_CROSS3'] = QA.CROSS(MTM['MAMTM'], MTM['MARK'])
            MTM['MTM_CROSS4'] = QA.CROSS(MTM['MARK'], MTM['MAMTM'])
        except:
            MTM = pd.DataFrame()
        try:
            EXPMA = data.add_func(QA.QA_indicator_EXPMA)
            EXPMA['MA1'] = data.data['close']/EXPMA['MA1']-1
            EXPMA['MA2'] = data.data['close']/EXPMA['MA2']-1
            EXPMA['MA3'] = data.data['close']/EXPMA['MA3']-1
            EXPMA['MA4'] = data.data['close']/EXPMA['MA4']-1
        except:
            EXPMA = pd.DataFrame()
        try:
            CHO = data.add_func(QA.QA_indicator_CHO)
        except:
            CHO = pd.DataFrame()
        try:
            BBI = data.add_func(QA.QA_indicator_BBI)
            BBI['BBI'] = BBI['BBI'] - data.data['close']
        except:
            BBI = pd.DataFrame()
        try:
            MFI = data.add_func(QA.QA_indicator_MFI)
        except:
            MFI = pd.DataFrame()
        try:
            ATR = data.add_func(QA.QA_indicator_ATR)
        except:
            ATR = pd.DataFrame()
        try:
            SKDJ = data.add_func(QA.QA_indicator_SKDJ)
            SKDJ['SKDJ_CROSS1'] = QA.CROSS(SKDJ['SKDJ_D'], SKDJ['SKDJ_K'])
            SKDJ['SKDJ_CROSS2'] = QA.CROSS(SKDJ['SKDJ_K'], SKDJ['SKDJ_D'])
        except:
            SKDJ = pd.DataFrame()
        try:
            DDI = data.add_func(QA.QA_indicator_DDI)
            DDI['DDI_C'] = DDI['DDI']/QA.REF(DDI['DDI'], 1)-1
            DDI['AD_C'] = DDI['AD']/QA.REF(DDI['AD'], 1)-1
            DDI['ADDI_C'] = DDI['ADDI']/QA.REF(DDI['ADDI'], 1)-1
        except:
            DDI = pd.DataFrame()
        try:
            shadow = data.add_func(QA.QA_indicator_shadow)
        except:
            shadow = pd.DataFrame()
        try:
            MA = data.add_func(QA.QA_indicator_MA,5,10,20,60,120,180)
            MA['MA5'] = data.data['close']/MA['MA5']-1
            MA['MA10'] = data.data['close']/MA['MA10']-1
            MA['MA20'] = data.data['close']/MA['MA20']-1
            MA['MA60'] = data.data['close']/MA['MA60']-1
            MA['MA120'] = data.data['close']/MA['MA120']-1
            MA['MA180'] = data.data['close']/MA['MA180']-1
        except:
            MA = pd.DataFrame()
        try:
            CDL2CROWS = data.add_func(QA.QAIndicator.talib_indicators.CDL2CROWS)
        except:
            CDL2CROWS = pd.DataFrame()
        try:
            CDL3BLACKCROWS = data.add_func(QA.QAIndicator.talib_indicators.CDL3BLACKCROWS)
        except:
            CDL3BLACKCROWS = pd.DataFrame()
        try:
            CDL3INSIDE = data.add_func(QA.QAIndicator.talib_indicators.CDL3INSIDE)
        except:
            CDL3INSIDE = pd.DataFrame()
        try:
            CDL3LINESTRIKE = data.add_func(QA.QAIndicator.talib_indicators.CDL3LINESTRIKE)
        except:
            CDL3LINESTRIKE = pd.DataFrame()
        try:
            CDL3OUTSIDE = data.add_func(QA.QAIndicator.talib_indicators.CDL3OUTSIDE)
        except:
            CDL3OUTSIDE = pd.DataFrame()
        try:
            CDL3STARSINSOUTH = data.add_func(QA.QAIndicator.talib_indicators.CDL3STARSINSOUTH)
        except:
            CDL3STARSINSOUTH = pd.DataFrame()
        try:
            CDL3WHITESOLDIERS = data.add_func(QA.QAIndicator.talib_indicators.CDL3WHITESOLDIERS)
        except:
            CDL3WHITESOLDIERS = pd.DataFrame()
        try:
            CDLABANDONEDBABY = data.add_func(QA.QAIndicator.talib_indicators.CDLABANDONEDBABY)
        except:
            CDLABANDONEDBABY = pd.DataFrame()
        try:
            CDLADVANCEBLOCK = data.add_func(QA.QAIndicator.talib_indicators.CDLADVANCEBLOCK)
        except:
            CDLADVANCEBLOCK = pd.DataFrame()
        try:
            CDLBELTHOLD = data.add_func(QA.QAIndicator.talib_indicators.CDLBELTHOLD)
        except:
            CDLBELTHOLD = pd.DataFrame()
        try:
            CDLBREAKAWAY = data.add_func(QA.QAIndicator.talib_indicators.CDLBREAKAWAY)
        except:
            CDLBREAKAWAY = pd.DataFrame()
        try:
            CDLCLOSINGMARUBOZU = data.add_func(QA.QAIndicator.talib_indicators.CDLCLOSINGMARUBOZU)
        except:
            CDLCLOSINGMARUBOZU = pd.DataFrame()
        try:
            CDLCONCEALBABYSWALL = data.add_func(QA.QAIndicator.talib_indicators.CDLCONCEALBABYSWALL)
        except:
            CDLCONCEALBABYSWALL = pd.DataFrame()
        try:
            CDLCOUNTERATTACK = data.add_func(QA.QAIndicator.talib_indicators.CDLCOUNTERATTACK)
        except:
            CDLCOUNTERATTACK = pd.DataFrame()
        try:
            CDLDARKCLOUDCOVER = data.add_func(QA.QAIndicator.talib_indicators.CDLDARKCLOUDCOVER)
        except:
            CDLDARKCLOUDCOVER = pd.DataFrame()
        try:
            CDLDOJI = data.add_func(QA.QAIndicator.talib_indicators.CDLDOJI)
        except:
            CDLDOJI = pd.DataFrame()
        try:
            CDLDOJISTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLDOJISTAR)
        except:
            CDLDOJISTAR = pd.DataFrame()
        try:
            CDLDRAGONFLYDOJI = data.add_func(QA.QAIndicator.talib_indicators.CDLDRAGONFLYDOJI)
        except:
            CDLDRAGONFLYDOJI = pd.DataFrame()
        try:
            CDLENGULFING = data.add_func(QA.QAIndicator.talib_indicators.CDLENGULFING)
        except:
            CDLENGULFING = pd.DataFrame()
        try:
            CDLEVENINGDOJISTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLEVENINGDOJISTAR)
        except:
            CDLEVENINGDOJISTAR = pd.DataFrame()
        try:
            CDLEVENINGSTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLEVENINGSTAR)
        except:
            CDLEVENINGSTAR = pd.DataFrame()
        try:
            CDLGAPSIDESIDEWHITE = data.add_func(QA.QAIndicator.talib_indicators.CDLGAPSIDESIDEWHITE)
        except:
            CDLGAPSIDESIDEWHITE = pd.DataFrame()
        try:
            CDLGRAVESTONEDOJI = data.add_func(QA.QAIndicator.talib_indicators.CDLGRAVESTONEDOJI)
        except:
            CDLGRAVESTONEDOJI = pd.DataFrame()
        try:
            CDLHAMMER = data.add_func(QA.QAIndicator.talib_indicators.CDLHAMMER)
        except:
            CDLHAMMER = pd.DataFrame()
        try:
            CDLHANGINGMAN = data.add_func(QA.QAIndicator.talib_indicators.CDLHANGINGMAN)
        except:
            CDLHANGINGMAN = pd.DataFrame()
        try:
            CDLHARAMI = data.add_func(QA.QAIndicator.talib_indicators.CDLHARAMI)
        except:
            CDLHARAMI = pd.DataFrame()
        try:
            CDLHARAMICROSS = data.add_func(QA.QAIndicator.talib_indicators.CDLHARAMICROSS)
        except:
            CDLHARAMICROSS = pd.DataFrame()
        try:
            CDLHIGHWAVE = data.add_func(QA.QAIndicator.talib_indicators.CDLHIGHWAVE)
        except:
            CDLHIGHWAVE = pd.DataFrame()
        try:
            CDLHIKKAKE = data.add_func(QA.QAIndicator.talib_indicators.CDLHIKKAKE)
        except:
            CDLHIKKAKE = pd.DataFrame()
        try:
            CDLHIKKAKEMOD = data.add_func(QA.QAIndicator.talib_indicators.CDLHIKKAKEMOD)
        except:
            CDLHIKKAKEMOD = pd.DataFrame()
        try:
            CDLHOMINGPIGEON = data.add_func(QA.QAIndicator.talib_indicators.CDLHOMINGPIGEON)
        except:
            CDLHOMINGPIGEON = pd.DataFrame()
        try:
            CDLIDENTICAL3CROWS = data.add_func(QA.QAIndicator.talib_indicators.CDLIDENTICAL3CROWS)
        except:
            CDLIDENTICAL3CROWS = pd.DataFrame()
        try:
            CDLINNECK = data.add_func(QA.QAIndicator.talib_indicators.CDLINNECK)
        except:
            CDLINNECK = pd.DataFrame()
        try:
            CDLINVERTEDHAMMER = data.add_func(QA.QAIndicator.talib_indicators.CDLINVERTEDHAMMER)
        except:
            CDLINVERTEDHAMMER = pd.DataFrame()
        try:
            CDLKICKING = data.add_func(QA.QAIndicator.talib_indicators.CDLKICKING)
        except:
            CDLKICKING = pd.DataFrame()
        try:
            CDLKICKINGBYLENGTH = data.add_func(QA.QAIndicator.talib_indicators.CDLKICKINGBYLENGTH)
        except:
            CDLKICKINGBYLENGTH = pd.DataFrame()
        try:
            CDLLADDERBOTTOM = data.add_func(QA.QAIndicator.talib_indicators.CDLLADDERBOTTOM)
        except:
            CDLLADDERBOTTOM = pd.DataFrame()
        try:
            CDLLONGLEGGEDDOJI = data.add_func(QA.QAIndicator.talib_indicators.CDLLONGLEGGEDDOJI)
        except:
            CDLLONGLEGGEDDOJI = pd.DataFrame()
        try:
            CDLLONGLINE = data.add_func(QA.QAIndicator.talib_indicators.CDLLONGLINE)
        except:
            CDLLONGLINE = pd.DataFrame()
        try:
            CDLMARUBOZU = data.add_func(QA.QAIndicator.talib_indicators.CDLMARUBOZU)
        except:
            CDLMARUBOZU = pd.DataFrame()
        try:
            CDLMATCHINGLOW = data.add_func(QA.QAIndicator.talib_indicators.CDLMATCHINGLOW)
        except:
            CDLMATCHINGLOW = pd.DataFrame()
        try:
            CDLMATHOLD = data.add_func(QA.QAIndicator.talib_indicators.CDLMATHOLD)
        except:
            CDLMATHOLD = pd.DataFrame()
        try:
            CDLMORNINGDOJISTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLMORNINGDOJISTAR)
        except:
            CDLMORNINGDOJISTAR = pd.DataFrame()
        try:
            CDLMORNINGSTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLMORNINGSTAR)
        except:
            CDLMORNINGSTAR = pd.DataFrame()
        try:
            CDLONNECK = data.add_func(QA.QAIndicator.talib_indicators.CDLONNECK)
        except:
            CDLONNECK = pd.DataFrame()
        try:
            CDLPIERCING = data.add_func(QA.QAIndicator.talib_indicators.CDLPIERCING)
        except:
            CDLPIERCING = pd.DataFrame()
        try:
            CDLRICKSHAWMAN = data.add_func(QA.QAIndicator.talib_indicators.CDLRICKSHAWMAN)
        except:
            CDLRICKSHAWMAN = pd.DataFrame()
        try:
            CDLRISEFALL3METHODS = data.add_func(QA.QAIndicator.talib_indicators.CDLRISEFALL3METHODS)
        except:
            CDLRISEFALL3METHODS = pd.DataFrame()
        try:
            CDLSEPARATINGLINES = data.add_func(QA.QAIndicator.talib_indicators.CDLSEPARATINGLINES)
        except:
            CDLSEPARATINGLINES = pd.DataFrame()
        try:
            CDLSHOOTINGSTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLSHOOTINGSTAR)
        except:
            CDLSHOOTINGSTAR = pd.DataFrame()
        try:
            CDLSHORTLINE = data.add_func(QA.QAIndicator.talib_indicators.CDLSHORTLINE)
        except:
            CDLSHORTLINE = pd.DataFrame()
        try:
            CDLSPINNINGTOP = data.add_func(QA.QAIndicator.talib_indicators.CDLSPINNINGTOP)
        except:
            CDLSPINNINGTOP = pd.DataFrame()
        try:
            CDLSTALLEDPATTERN = data.add_func(QA.QAIndicator.talib_indicators.CDLSTALLEDPATTERN)
        except:
            CDLSTALLEDPATTERN = pd.DataFrame()
        try:
            CDLSTICKSANDWICH = data.add_func(QA.QAIndicator.talib_indicators.CDLSTICKSANDWICH)
        except:
            CDLSTICKSANDWICH = pd.DataFrame()
        try:
            CDLTAKURI = data.add_func(QA.QAIndicator.talib_indicators.CDLTAKURI)
        except:
            CDLTAKURI = pd.DataFrame()
        try:
            CDLTASUKIGAP = data.add_func(QA.QAIndicator.talib_indicators.CDLTASUKIGAP)
        except:
            CDLTASUKIGAP = pd.DataFrame()
        try:
            CDLTHRUSTING = data.add_func(QA.QAIndicator.talib_indicators.CDLTHRUSTING)
        except:
            CDLTHRUSTING = pd.DataFrame()
        try:
            CDLTRISTAR = data.add_func(QA.QAIndicator.talib_indicators.CDLTRISTAR)
        except:
            CDLTRISTAR = pd.DataFrame()
        try:
            CDLUNIQUE3RIVER = data.add_func(QA.QAIndicator.talib_indicators.CDLUNIQUE3RIVER)
        except:
            CDLUNIQUE3RIVER = pd.DataFrame()
        try:
            CDLUPSIDEGAP2CROWS = data.add_func(QA.QAIndicator.talib_indicators.CDLUPSIDEGAP2CROWS)
        except:
            CDLUPSIDEGAP2CROWS = pd.DataFrame()
        try:
            CDLXSIDEGAP3METHODS = data.add_func(QA.QAIndicator.talib_indicators.CDLXSIDEGAP3METHODS)
        except:
            CDLXSIDEGAP3METHODS = pd.DataFrame()

        res =pd.concat([VR,VRSI,VSTD,BOLL,MIKE,ASI,OBV,PVT,VPT,KDJ,WR,ROC,RSI,CCI,BIAS,OSC,
                        ADTM,MACD,DMI,DMA,PBX,MTM,EXPMA,CHO,BBI,MFI,ATR,SKDJ,DDI,#shadow,
                        MA,
                        CDL2CROWS,CDL3BLACKCROWS,CDL3INSIDE,CDL3LINESTRIKE,CDL3OUTSIDE,
                        CDL3STARSINSOUTH,CDL3WHITESOLDIERS,CDLABANDONEDBABY,CDLADVANCEBLOCK,
                        CDLBELTHOLD,CDLBREAKAWAY,CDLCLOSINGMARUBOZU,CDLCONCEALBABYSWALL,
                        CDLCOUNTERATTACK,CDLDARKCLOUDCOVER,CDLDOJI,CDLDOJISTAR,CDLDRAGONFLYDOJI,
                        CDLENGULFING,CDLEVENINGDOJISTAR,CDLEVENINGSTAR,CDLGAPSIDESIDEWHITE,
                        CDLGRAVESTONEDOJI,CDLHAMMER,CDLHANGINGMAN,CDLHARAMI,CDLHARAMICROSS,
                        CDLHIGHWAVE,CDLHIKKAKE,CDLHIKKAKEMOD,CDLHOMINGPIGEON,CDLIDENTICAL3CROWS,
                        CDLINNECK,CDLINVERTEDHAMMER,CDLKICKING,CDLKICKINGBYLENGTH,CDLLADDERBOTTOM,
                        CDLLONGLEGGEDDOJI,CDLLONGLINE,CDLMARUBOZU,CDLMATCHINGLOW,CDLMATHOLD,
                        CDLMORNINGDOJISTAR,CDLMORNINGSTAR,CDLONNECK,CDLPIERCING,CDLRICKSHAWMAN,
                        CDLRISEFALL3METHODS,CDLSEPARATINGLINES,CDLSHOOTINGSTAR,CDLSHORTLINE,
                        CDLSPINNINGTOP,CDLSTALLEDPATTERN,CDLSTICKSANDWICH,CDLTAKURI,CDLTASUKIGAP,
                        CDLTHRUSTING,CDLTRISTAR,CDLUNIQUE3RIVER,CDLUPSIDEGAP2CROWS,CDLXSIDEGAP3METHODS],
                       axis=1).dropna(how='all').reset_index()
        res = res.ix[:, ['date','code','VRSI','VRSI_C','BOLL','UB','LB','WIDTH','BOLL_CROSS1','BOLL_CROSS2',
                       'BOLL_CROSS3','BOLL_CROSS4','WR','MR','SR','WS','MS','SS','ASI','ASIT',
                       'VPT','MAVPT','VPT_CROSS1','VPT_CROSS2','VPT_CROSS3','VPT_CROSS4','KDJ_K',
                       'KDJ_D','KDJ_J','KDJ_CROSS1','KDJ_CROSS2','WR1','WR2','WR_CROSS1','WR_CROSS2','ROC','ROCMA','RSI1',
                       'RSI2','RSI3','RSI_CROSS1','RSI_CROSS2','RSI1_C','RSI2_C','RSI3_C','BIAS1','BIAS2','BIAS3','BIAS_CROSS1','BIAS_CROSS2',
                        'OSC','MAOSC','OSC_CROSS1','OSC_CROSS2','OSC_CROSS3','OSC_CROSS4','ADTM','MAADTM','DI1','DI2',
                       'ADX','ADXR','ADX_C','DI_M','DI_CROSS1','DI_CROSS2','ADX_CROSS1','ADX_CROSS2',
                       'DDD','AMA','DMA_CROSS1','DMA_CROSS2','PBX1','PBX2','PBX3','PBX4','PBX5','PBX6',
                       'PBX1_C','PBX2_C','PBX3_C','PBX4_C','PBX5_C','PBX6_C','MA1','MA2','MA3','MA4','CHO',
                       'MACHO','BBI','MFI','TR','ATR','RSV','SKDJ_K','SKDJ_D','SKDJ_CROSS1','SKDJ_CROSS2',
                       'DDI','ADDI','AD','DDI_C','AD_C','ADDI_C','MA5','MA10','MA20','MA60','MA120','MA180',
                       'CDL2CROWS','CDL3BLACKCROWS','CDL3INSIDE','CDL3LINESTRIKE','CDL3OUTSIDE','CDL3STARSINSOUTH',
                       'CDL3WHITESOLDIERS','CDLABANDONEDBABY','CDLADVANCEBLOCK','CDLBELTHOLD','CDLBREAKAWAY',
                       'CDLCLOSINGMARUBOZU','CDLCONCEALBABYSWALL','CDLCOUNTERATTACK','CDLDARKCLOUDCOVER',
                       'CDLDOJI','CDLDOJISTAR','CDLDRAGONFLYDOJI','CDLENGULFING','CDLEVENINGDOJISTAR',
                       'CDLEVENINGSTAR','CDLGAPSIDESIDEWHITE','CDLGRAVESTONEDOJI','CDLHAMMER','CDLHANGINGMAN',
                       'CDLHARAMI','CDLHARAMICROSS','CDLHIGHWAVE','CDLHIKKAKE','CDLHIKKAKEMOD','CDLHOMINGPIGEON',
                       'CDLIDENTICAL3CROWS','CDLINNECK','CDLINVERTEDHAMMER','CDLKICKING','CDLKICKINGBYLENGTH',
                       'CDLLADDERBOTTOM','CDLLONGLEGGEDDOJI','CDLLONGLINE','CDLMARUBOZU','CDLMATCHINGLOW',
                       'CDLMATHOLD','CDLMORNINGDOJISTAR','CDLMORNINGSTAR','CDLONNECK','CDLPIERCING','CDLRICKSHAWMAN',
                       'CDLRISEFALL3METHODS','CDLSEPARATINGLINES','CDLSHOOTINGSTAR','CDLSHORTLINE','CDLSPINNINGTOP',
                       'CDLSTALLEDPATTERN','CDLSTICKSANDWICH','CDLTAKURI','CDLTASUKIGAP','CDLTHRUSTING','CDLTRISTAR',
                       'CDLUNIQUE3RIVER','CDLUPSIDEGAP2CROWS','CDLXSIDEGAP3METHODS']]
        data = res.assign(date_stamp=res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
