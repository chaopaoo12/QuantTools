from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv,QA_fetch_stock_day_adv
import QUANTAXIS as QA
import pandas as pd

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
        except:
            VRSI = pd.DataFrame()
        try:
            VSTD = data.add_func(QA.QA_indicator_VSTD)
        except:
            VSTD = pd.DataFrame()
        try:
            BOLL = data.add_func(QA.QA_indicator_BOLL)
        except:
            BOLL = pd.DataFrame()
        try:
            MIKE = data.add_func(QA.QA_indicator_MIKE)
        except:
            MIKE = pd.DataFrame()
        try:
            ASI = data.add_func(QA.QA_indicator_ASI)
        except:
            ASI = pd.DataFrame()
        try:
            OBV = data.add_func(QA.QA_indicator_OBV)
        except:
            OBV = pd.DataFrame()
        try:
            PVT = data.add_func(QA.QA_indicator_PVT)
        except:
            PVT = pd.DataFrame()
        try:
            VPT = data.add_func(QA.QA_indicator_VPT)
        except:
            VPT = pd.DataFrame()
        try:
            KDJ = data.add_func(QA.QA_indicator_KDJ)
        except:
            KDJ = pd.DataFrame()
        try:
            WR = data.add_func(QA.QA_indicator_WR,1,2)
        except:
            WR = pd.DataFrame()
        try:
            ROC = data.add_func(QA.QA_indicator_ROC)
        except:
            ROC = pd.DataFrame()
        try:
            RSI = data.add_func(QA.QA_indicator_RSI)
        except:
            RSI = pd.DataFrame()
        try:
            CCI = data.add_func(QA.QA_indicator_CCI)['CCI']
        except:
            CCI = pd.DataFrame()
        try:
            BIAS = data.add_func(QA.QA_indicator_BIAS,6,12,24)
        except:
            BIAS = pd.DataFrame()
        try:
            OSC = data.add_func(QA.QA_indicator_OSC)
        except:
            OSC = pd.DataFrame()
        try:
            ADTM = data.add_func(QA.QA_indicator_ADTM)
        except:
            ADTM = pd.DataFrame()
        try:
            MACD = data.add_func(QA.QA_indicator_MACD)
        except:
            MACD = pd.DataFrame()
        try:
            DMI = data.add_func(QA.QA_indicator_DMI)
        except:
            DMI = pd.DataFrame()
        try:
            DMA = data.add_func(QA.QA_indicator_DMA)
        except:
            DMA = pd.DataFrame()
        try:
            PBX = data.add_func(QA.QA_indicator_PBX)
        except:
            PBX = pd.DataFrame()
        try:
            MTM = data.add_func(QA.QA_indicator_MTM)
        except:
            MTM = pd.DataFrame()
        try:
            EXPMA = data.add_func(QA.QA_indicator_EXPMA)
        except:
            EXPMA = pd.DataFrame()
        try:
            CHO = data.add_func(QA.QA_indicator_CHO)
        except:
            CHO = pd.DataFrame()
        try:
            BBI = data.add_func(QA.QA_indicator_BBI)
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
        except:
            SKDJ = pd.DataFrame()
        try:
            DDI = data.add_func(QA.QA_indicator_DDI)
        except:
            DDI = pd.DataFrame()
        try:
            shadow = data.add_func(QA.QA_indicator_shadow)
        except:
            shadow = pd.DataFrame()
        try:
            MA = data.add_func(QA.QA_indicator_MA,5,10,20,60,120,180)
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
                        ADTM,MACD,DMI,DMA,PBX,MTM,EXPMA,CHO,BBI,MFI,ATR,SKDJ,DDI,shadow,MA,
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
                       axis=1)
        return(res.dropna(how='all'))
