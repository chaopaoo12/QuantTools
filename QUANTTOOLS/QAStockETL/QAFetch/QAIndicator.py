from QUANTAXIS import (QA_indicator_VR,QA_indicator_VRSI,QA_indicator_VSTD,
                       QA_indicator_BOLL,QA_indicator_MA,QA_indicator_MA_VOL,QA_indicator_ASI,
                       QA_indicator_OBV,QA_indicator_PVT,QA_indicator_VPT,QA_indicator_KDJ,
                       QA_indicator_WR,QA_indicator_ROC,QA_indicator_RSI,QA_indicator_CCI,
                       QA_indicator_BIAS,QA_indicator_ADTM,QA_indicator_DMI,QA_indicator_PBX,
                       QA_indicator_BBI,QA_indicator_MFI,QA_indicator_DDI,QA_indicator_shadow)
from QUANTAXIS.QAIndicator.base import CROSS,REF,MA,SUM,LLV,HHV,ABS,STD,MAX,MIN
from QUANTAXIS.QAIndicator.talib_indicators import (CDL2CROWS,CDL3BLACKCROWS,CDL3INSIDE,CDL3LINESTRIKE,CDL3OUTSIDE,
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
                                                    CDLTHRUSTING,CDLTRISTAR,CDLUNIQUE3RIVER,CDLUPSIDEGAP2CROWS,CDLXSIDEGAP3METHODS)
from QUANTAXIS.QAIndicator.talib_indicators import (SAR)
from scipy import stats
import numpy as np
import pandas as pd
import math

def QA_indicator_MACD(DataFrame, short=12, long=26, mid=9):
    """
    MACD CALC
    """
    CLOSE = DataFrame['close']

    DIF = (MA(CLOSE, short)-MA(CLOSE, long))/((MA(CLOSE, short)+MA(CLOSE, long))/2)*100
    DEA = MA(DIF, mid)
    MACD = (DIF-DEA)*2

    return pd.DataFrame({'DIF': DIF, 'DEA': DEA, 'MACD': MACD})

def QA_indicator_DMA(DataFrame, M1=10, M2=50, M3=10):
    """
    平均线差 DMA
    """
    CLOSE = DataFrame.close
    DDD = MA(CLOSE, M1) / MA(CLOSE, M2) - 1
    AMA = MA(DDD, M3)
    return pd.DataFrame({
        'DDD': DDD, 'AMA': AMA
    })

def QA_indicator_MTM(DataFrame, N=12, M=6):
    '动量线'
    C = DataFrame.close
    mtm = C / REF(C, N) - 1
    MTMMA = MA(mtm, M)
    DICT = {'MTM': mtm, 'MTMMA': MTMMA}

    return pd.DataFrame(DICT)

def QA_indicator_CHO(DataFrame, N1=10, N2=20, M=6):
    """
    佳庆指标 CHO
    """
    HIGH = DataFrame.high
    LOW = DataFrame.low
    CLOSE = DataFrame.close
    VOL = DataFrame.volume
    MID = SUM(VOL*(2*CLOSE-HIGH-LOW)/(HIGH+LOW), 0)
    CHO = MA(MID, N1)/MA(MID, N2)-1
    MACHO = MA(CHO, M)
    return pd.DataFrame({
        'CHO': CHO, 'MACHO': MACHO
    })

def QA_indicator_OSC(DataFrame, N=20, M=6):
    """变动速率线

    震荡量指标OSC，也叫变动速率线。属于超买超卖类指标,是从移动平均线原理派生出来的一种分析指标。

    它反应当日收盘价与一段时间内平均收盘价的差离值,从而测出股价的震荡幅度。

    按照移动平均线原理，根据OSC的值可推断价格的趋势，如果远离平均线，就很可能向平均线回归。
    """
    C = DataFrame['close']
    OS = (C / MA(C, N) - 1)
    MAOSC = MA(OS, M)
    DICT = {'OSC': OS, 'MAOSC': MAOSC}

    return pd.DataFrame(DICT)

def QA_indicator_CMI(DataFrame, N=6):
    """
    1.指标>=20 时, 趋势区间；
    2.指标<20 时, 无趋势；

    """
    CLOSE = DataFrame['close']
    LOWV = LLV(DataFrame['low'], N)
    HIGHV = HHV(DataFrame['high'], N)
    CMI = ABS((CLOSE - REF(CLOSE, N - 1)) / (HIGHV - LOWV)) * 100
    DICT = {'CMI': CMI}

    return pd.DataFrame(DICT)

def QA_indicator_SKDJ(DataFrame, N=9, M=3):
    """
    1.指标>80 时，回档机率大；指标<20 时，反弹机率大；
    2.K在20左右向上交叉D时，视为买进信号参考；
    3.K在80左右向下交叉D时，视为卖出信号参考；
    4.SKDJ波动于50左右的任何讯号，其作用不大。

    """
    CLOSE = DataFrame['close']
    LOWV = LLV(DataFrame['low'], N)
    HIGHV = HHV(DataFrame['high'], N)
    RSV = MA((CLOSE - LOWV) / (HIGHV - LOWV) * 100, M)
    K = MA(RSV, M)
    D = MA(K, M)
    DICT = {'RSV': RSV, 'SKDJ_K': K, 'SKDJ_D': D}

    return pd.DataFrame(DICT)

def ohlc(data,N=7):
    data['open'] = data['open'].rolling(window=N,min_periods=1).apply(lambda x:x[0],raw=True)
    data['high'] = data['high'].rolling(window=N,min_periods=1).apply(lambda x:x.max(),raw=True)
    data['low'] = data['low'].rolling(window=N,min_periods=1).apply(lambda x:x.min(),raw=True)
    data['close'] = data['close'].rolling(window=N,min_periods=1).apply(lambda x:x[-1],raw=True)
    data['volume'] = data['volume'].rolling(window=N,min_periods=1).apply(lambda x:x.sum(),raw=True)
    data['amount'] = data['amount'].rolling(window=N,min_periods=1).apply(lambda x:x.sum(),raw=True)
    return(data)

def rolling_ols(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(math.atan(model.slope)*180/math.pi)

def spc(data, N= 5):
    data[['MA5_C','MA15_C','MA30_C','MA60_C','GMMA3_C','GMMA15_C','GMMA30_C',
          'MA_VOL5_C','MA_VOL60_C','GMMA_VOL3_C','GMMA_VOL15_C','GMMA_VOL30_C']]= data.rolling(window=N).agg({'MA5':rolling_ols,
                                                                                                              'MA15':rolling_ols,
                                                                                                              'MA30':rolling_ols,
                                   'MA60':rolling_ols,
                                   'GMMA3':rolling_ols,
                                   'GMMA15':rolling_ols,
                                   'GMMA30':rolling_ols,
                                   'MA_VOL5':rolling_ols,
                                   'MA_VOL60':rolling_ols,
                                   'GMMA_VOL5':rolling_ols,
                                   'GMMA_VOL15':rolling_ols,
                                   'GMMA_VOL30':rolling_ols,
    })
    return(data)

def MIKE_NEW(DataFrame,MIKE_N=12,MA_N=5):
    HIGH = DataFrame.high
    LOW = DataFrame.low
    CLOSE = DataFrame.close
    TYP = (HIGH+LOW+CLOSE)/3
    LL = LLV(LOW, MIKE_N)
    HH = HHV(HIGH, MIKE_N)
    WR = TYP+(TYP-LL)
    MR = TYP+(HH-LL)
    SR = 2*HH-LL
    WS = TYP-(HH-TYP)
    MS = TYP-(HH-LL)
    SS = 2*LL-HH
    MA5 = MA(CLOSE, MA_N)
    UB = MA5 + 2 * STD(CLOSE, 20)
    LB = MA5 - 2 * STD(CLOSE, 20)
    MIKE_WRSC = CROSS(UB, WR)
    MIKE_WRJC = CROSS(WR, UB)
    MIKE_WSSC = CROSS(LB, WS)
    MIKE_WSJC = CROSS(WS, LB)
    MIKE_TR = ((MIKE_WSJC + MIKE_WRJC) >0) *1 + 2 *1* ((MIKE_WSSC+MIKE_WRSC) > 0)
    MIKE_TR = MIKE_TR.replace(0, np.nan)
    MIKE_TR = MIKE_TR.groupby('code').fillna(method='ffill').replace(2,0).fillna(0)
    MIKE_BOLL = ((WR > UB) & (WS > LB)) *1
    return pd.DataFrame({'WR':WR,'MR':MR,'SR':SR,'WS':WS,'MS':MS,'SS':SS
                            ,'MIKE_WRSC':MIKE_WRSC,'MIKE_WRJC':MIKE_WRJC,'MIKE_WSSC':MIKE_WSSC,'MIKE_WSJC':MIKE_WSJC,
                         'MIKE_TR':MIKE_TR,'MIKE_BOLL':MIKE_BOLL})

def indicator_ATR(DataFrame, N=14):
    """
    输出TR:(最高价-最低价)和昨收-最高价的绝对值的较大值和昨收-最低价的绝对值的较大值
    输出真实波幅:TR的N日简单移动平均
    算法：今日振幅、今日最高与昨收差价、今日最低与昨收差价中的最大值，为真实波幅，求真实波幅的N日移动平均

    参数：N　天数，一般取14

    """
    C = DataFrame['close']
    H = DataFrame['high']
    L = DataFrame['low']
    TR = MAX(MAX((H - L), ABS(REF(C, 1) - H)), ABS(REF(C, 1) - L))
    atr = MA(TR, N)
    atrc = atr / REF(C, 1)
    return pd.DataFrame({'TR': TR, 'ATR': atr, 'ATRR':atrc})

def function1(a, b):
    if a > b:
        return 1
    elif a < b:
        return -1
    else:
        return 0

def get_indicator(data, type='day'):
    try:
        # todo
        #A.低价区域：70~40——为可买进区域
        #B.安全区域：150~80——正常分布区域
        #C.获利区域：450~160——应考虑获利了结
        #D.警戒区域：450以上——股价已过高
        #2.在低价区域中，VR值止跌回升，可买进，
        #3.在VR>160时，股价上扬，VR值见顶，可卖出，
        #1．VR指标在低价区域准确度较高，当VR>160时有失真可能，特别是在350~400高档区，有时会发生将股票卖出后，股价仍续涨的现象，此时可以配合PSY心理线指标来化解疑难。
        #2．VR低于40的形态，运用在个股走势上，常发生股价无法有效反弹的效应，随后VR只维持在40~60之间徘徊。因而，此种讯号较适宜应用在指数方面，并且配合ADR、OBOS……等指标使用效果非常好。
        VR = data.add_func(QA_indicator_VR)['VR']
    except:
        VR = data.data.assign(VR=None)['VR']
    try:
        VRSI = data.add_func(QA_indicator_VRSI)
        VRSI['VRSI_C'] = VRSI['VRSI']/REF(VRSI['VRSI'], 1)-1
    except:
        VRSI = data.data.assign(VRSI=None,VRSI_C=None)[['VRSI','VRSI_C']]
    try:
        VSTD = data.add_func(QA_indicator_VSTD)
        VSTD['VSTD'] = data['volume']/VSTD['VSTD']-1
    except:
        VSTD = data.data.assign(VSTD=None)['VSTD']
    try:
        BOLL = data.add_func(QA_indicator_BOLL)
        BOLL['WIDTH'] = (BOLL['UB']-BOLL['LB'])/BOLL['BOLL']
        BOLL['BOLL'] = data['close'] / BOLL['BOLL'] - 1
        BOLL['UB'] = data['close'] / BOLL['UB'] - 1
        BOLL['LB'] = data['close'] / BOLL['LB'] - 1
    except:
        BOLL = data.data.assign(BOLL=None,UB=None,LB=None,WIDTH=None,
                                BOLL_CROSS1=0,BOLL_CROSS2=0,BOLL_CROSS3=0,
                                BOLL_CROSS4=0)[['BOLL','UB','LB','WIDTH']]
    try:
        MIKE = data.add_func(MIKE_NEW)
    except:
        MIKE = data.data.assign(WR=None,MR=None,SR=None,WS=None,MS=None,SS=None,
                                MIKE_WRSC=0,MIKE_WRJC=0,MIKE_WSSC=0,MIKE_WSJC=0,MIKE_TR=0)[['WR','MR','SR','WS','MS','SS',
                                                                                            'MIKE_WRSC','MIKE_WRJC','MIKE_WSSC','MIKE_WSJC','MIKE_TR']]
    try:
        MA = data.add_func(QA_indicator_MA,3,5,8,10,12,15,20,30,35,40,45,50,60)
        MA['SHORT10'] = MA['MA5']/MA['MA10']-1
        MA['SHORT20'] = MA['MA10']/MA['MA20']-1
        MA['SHORT60'] = MA['MA10']/MA['MA60']-1
        MA['LONG60'] = MA['MA20']/MA['MA60']-1
        MA['SHORT_CROSS1'] = CROSS(MA['MA10'], MA['MA20'])
        MA['SHORT_CROSS2'] = CROSS(MA['MA20'], MA['MA10'])
        MA['LONG_CROSS1'] = CROSS(MA['MA20'], MA['MA60'])
        MA['LONG_CROSS2'] = CROSS(MA['MA60'], MA['MA20'])
        MA['LONG_AMOUNT'] = MA['MA20']-MA['MA60']
        MA['SHORT_AMOUNT'] = MA['MA10']-MA['MA20']

        MA['GMMA3'] = MA['MA3']/MA['MA15']-1
        MA['GMMA5'] = MA['MA5']/MA['MA15']-1
        MA['GMMA8'] = MA['MA8']/MA['MA15']-1
        MA['GMMA10'] = MA['MA10']/MA['MA15']-1
        MA['GMMA12'] = MA['MA12']/MA['MA15']-1
        MA['GMMA15'] = MA['MA15']/MA['MA30']-1

        MA['GMMA30'] = MA['MA30']/MA['MA60']-1
        MA['GMMA35'] = MA['MA35']/MA['MA60']-1
        MA['GMMA40'] = MA['MA40']/MA['MA60']-1
        MA['GMMA45'] = MA['MA45']/MA['MA60']-1
        MA['GMMA50'] = MA['MA50']/MA['MA60']-1

    except:
        MA = data.data.assign(MA3=None,MA5=None,MA8=None,MA10=None,MA12=None,MA15=None,MA20=None,
                              MA30=None,MA35=None,MA40=None,MA45=None,MA50=None,MA60=None)[['MA3','MA5','MA8','MA10','MA12','MA15','MA20','MA30','MA35','MA40','MA45','MA50','MA60']]
        MA['SHORT10'] = MA['MA5']/MA['MA10']-1
        MA['SHORT20'] = MA['MA10']/MA['MA20']-1
        MA['SHORT60'] = MA['MA10']/MA['MA60']-1
        MA['LONG60'] = MA['MA20']/MA['MA60']-1
        MA['SHORT_CROSS1'] = CROSS(MA['MA10'], MA['MA20'])
        MA['SHORT_CROSS2'] = CROSS(MA['MA20'], MA['MA10'])
        MA['LONG_CROSS1'] = CROSS(MA['MA20'], MA['MA60'])
        MA['LONG_CROSS2'] = CROSS(MA['MA60'], MA['MA20'])
        MA['LONG_AMOUNT'] = MA['MA20']-MA['MA60']
        MA['SHORT_AMOUNT'] = MA['MA10']-MA['MA20']

        MA['GMMA3'] = MA['MA3']/MA['MA15']-1
        MA['GMMA5'] = MA['MA5']/MA['MA15']-1
        MA['GMMA8'] = MA['MA8']/MA['MA15']-1
        MA['GMMA10'] = MA['MA10']/MA['MA15']-1
        MA['GMMA12'] = MA['MA12']/MA['MA15']-1
        MA['GMMA15'] = MA['MA15']/MA['MA30']-1

        MA['GMMA30'] = MA['MA30']/MA['MA60']-1
        MA['GMMA35'] = MA['MA35']/MA['MA60']-1
        MA['GMMA40'] = MA['MA40']/MA['MA60']-1
        MA['GMMA45'] = MA['MA45']/MA['MA60']-1
        MA['GMMA50'] = MA['MA50']/MA['MA60']-1

    try:
        MA_VOL = data.add_func(QA_indicator_MA_VOL,3,5,8,10,12,15,20,30,35,40,45,50,60)
        MA_VOL['SHORT10V'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL10']-1
        MA_VOL['SHORT20V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL20']-1
        MA_VOL['SHORT60V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL60']-1
        MA_VOL['LONG60V'] = MA_VOL['MA_VOL20']/MA_VOL['MA_VOL60']-1
        MA_VOL['SHORTV_CROSS1'] = CROSS(MA_VOL['MA_VOL10'], MA_VOL['MA_VOL20'])
        MA_VOL['SHORTV_CROSS2'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL10'])
        MA_VOL['LONGV_CROSS1'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL60'])
        MA_VOL['LONGV_CROSS2'] = CROSS(MA_VOL['MA_VOL60'], MA_VOL['MA_VOL20'])
        MA_VOL['LONGV_AMOUNT'] = MA_VOL['MA_VOL20']-MA_VOL['MA_VOL60']
        MA_VOL['SHORTV_AMOUNT'] = MA_VOL['MA_VOL10']-MA_VOL['MA_VOL20']

        MA_VOL['GMMA_VOL3'] = MA_VOL['MA_VOL3']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL5'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL8'] = MA_VOL['MA_VOL8']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL10'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL12'] = MA_VOL['MA_VOL12']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL15'] = MA_VOL['MA_VOL15']/MA_VOL['MA_VOL30']-1

        MA_VOL['GMMA_VOL30'] = MA_VOL['MA_VOL30']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL35'] = MA_VOL['MA_VOL35']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL40'] = MA_VOL['MA_VOL40']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL45'] = MA_VOL['MA_VOL45']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL50'] = MA_VOL['MA_VOL50']/MA_VOL['MA_VOL60']-1

    except:
        MA_VOL = data.data.assign(MA_VOL3=None,MA_VOL5=None,MA_VOL8=None,MA_VOL10=None,MA_VOL12=None,MA_VOL15=None,MA_VOL20=None,
                                  MA_VOL30=None,MA_VOL35=None,MA_VOL40=None,MA_VOL45=None,MA_VOL50=None,MA_VOL60=None)[['MA_VOL3','MA_VOL5','MA_VOL8','MA_VOL10','MA_VOL12','MA_VOL15','MA_VOL20','MA_VOL30','MA_VOL35','MA_VOL40','MA_VOL45','MA_VOL50','MA_VOL60']]
        MA_VOL['SHORT10V'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL10']-1
        MA_VOL['SHORT20V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL20']-1
        MA_VOL['SHORT60V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL60']-1
        MA_VOL['LONG60V'] = MA_VOL['MA_VOL20']/MA_VOL['MA_VOL60']-1
        MA_VOL['SHORTV_CROSS1'] = CROSS(MA_VOL['MA_VOL10'], MA_VOL['MA_VOL20'])
        MA_VOL['SHORTV_CROSS2'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL10'])
        MA_VOL['LONGV_CROSS1'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL60'])
        MA_VOL['LONGV_CROSS2'] = CROSS(MA_VOL['MA_VOL60'], MA_VOL['MA_VOL20'])
        MA_VOL['LONGV_AMOUNT'] = MA_VOL['MA_VOL20']-MA_VOL['MA_VOL60']
        MA_VOL['SHORTV_AMOUNT'] = MA_VOL['MA_VOL10']-MA_VOL['MA_VOL20']

        MA_VOL['GMMA_VOL3'] = MA_VOL['MA_VOL3']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL5'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL8'] = MA_VOL['MA_VOL8']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL10'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL12'] = MA_VOL['MA_VOL12']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL15'] = MA_VOL['MA_VOL15']/MA_VOL['MA_VOL30']-1

        MA_VOL['GMMA_VOL30'] = MA_VOL['MA_VOL30']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL35'] = MA_VOL['MA_VOL35']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL40'] = MA_VOL['MA_VOL40']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL45'] = MA_VOL['MA_VOL45']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL50'] = MA_VOL['MA_VOL50']/MA_VOL['MA_VOL60']-1

    try:
        ASI = data.add_func(QA_indicator_ASI)
    except:
        ASI = data.data.assign(ASI=None,ASIT=None)[['ASI','ASIT']]
    try:
        SAR_V = data.add_func(SAR,acceleration = 0.02, maximum = 0.2)
    except:
        SAR_V = data.data.assign(SAR=None)[['SAR']]
    try:
        OBV = data.add_func(QA_indicator_OBV)
        OBV['OBV_C'] = OBV['OBV']/REF(OBV['OBV'], 1)-1
    except:
        OBV = data.data.assign(OBV=None,OBV_C=None)[['OBV','OBV_C']]
    try:
        PVT = data.add_func(QA_indicator_PVT)
        PVT['PVT_C'] = PVT['PVT']/REF(PVT['PVT'], 1)-1
    except:
        PVT = data.data.assign(PVT=None,PVT_C=None)[['PVT','PVT_C']]
    try:
        VPT = data.add_func(QA_indicator_VPT)
        VPT['MARK'] = 0
        VPT['VPT_CROSS1'] = CROSS(VPT['VPT'], VPT['MARK'])
        VPT['VPT_CROSS2'] = CROSS(VPT['MARK'], VPT['VPT'])
        VPT['VPT_CROSS3'] = CROSS(VPT['MAVPT'], VPT['MARK'])
        VPT['VPT_CROSS4'] = CROSS(VPT['MARK'], VPT['MAVPT'])
    except:
        VPT = data.data.assign(VPT=None,VPT_CROSS1=0,VPT_CROSS2=0,VPT_CROSS3=0,
                               VPT_CROSS4=0,)[['VPT','VPT_CROSS1','VPT_CROSS2','VPT_CROSS3','VPT_CROSS4']]
    try:
        KDJ = data.add_func(QA_indicator_KDJ)
        KDJ['KDJ_CROSS1'] = CROSS(KDJ['KDJ_D'], KDJ['KDJ_K'])
        KDJ['KDJ_CROSS2'] = CROSS(KDJ['KDJ_K'], KDJ['KDJ_D'])
    except:
        KDJ = data.data.assign(KDJ_K=None,KDJ_D=None,KDJ_J=None,KDJ_CROSS1=0,
                               KDJ_CROSS2=0)[['KDJ_K','KDJ_D','KDJ_J','KDJ_CROSS1','KDJ_CROSS2']]
    try:
        WR = data.add_func(QA_indicator_WR,10,6)
        WR['WR_CROSS1'] = CROSS(WR['WR1'], WR['WR2'])
        WR['WR_CROSS2'] = CROSS(WR['WR2'], WR['WR1'])
    except:
        WR = data.data.assign(WR1=None,WR2=None,WR_CROSS1=0,
                              WR_CROSS2=0)[['WR1','WR2','WR_CROSS1','WR_CROSS2']]
    try:
        ROC = data.add_func(QA_indicator_ROC)
    except:
        ROC = data.data.assign(ROC=None,ROCMA=None)[['ROC','ROCMA']]
    try:
        RSI = data.add_func(QA_indicator_RSI)
        RSI['RSI1_C'] = RSI['RSI1']/REF(RSI['RSI1'], 1)-1
        RSI['RSI2_C'] = RSI['RSI2']/REF(RSI['RSI2'], 1)-1
        RSI['RSI3_C'] = RSI['RSI3']/REF(RSI['RSI3'], 1)-1
        RSI['RSI_CROSS1'] = CROSS(RSI['RSI1'], RSI['RSI3'])
        RSI['RSI_CROSS2'] = CROSS(RSI['RSI3'], RSI['RSI1'])
    except:
        RSI = data.data.assign(RSI1=None,RSI2=None,RSI3=None,
                               RSI1_C=None,RSI2_C=None,RSI3_C=None,
                               RSI_CROSS1=0,RSI_CROSS2=0)[['RSI1','RSI2','RSI3','RSI1_C','RSI2_C',
                                                           'RSI3_C','RSI_CROSS1','RSI_CROSS2']]
    try:
        CCI = data.add_func(QA_indicator_CCI)
        CCI['CCI_CROSS1'] = CROSS(CCI['CCI'], CCI['a'])
        CCI['CCI_CROSS2'] = CROSS(CCI['a'], CCI['CCI'])
        CCI['CCI_CROSS3'] = CROSS(CCI['CCI'], CCI['b'])
        CCI['CCI_CROSS4'] = CROSS(CCI['b'], CCI['CCI'])
    except:
        CCI = data.data.assign(CCI=None,
                               CCI_CROSS1=0,CCI_CROSS2=0,
                               CCI_CROSS3=0,CCI_CROSS4=0)[['CCI','CCI_CROSS1','CCI_CROSS2',
                                                           'CCI_CROSS3','CCI_CROSS4']]
    try:
        BIAS = data.add_func(QA_indicator_BIAS,6,12,24)
        BIAS['BIAS_CROSS1'] = CROSS(BIAS['BIAS1'], BIAS['BIAS3'])
        BIAS['BIAS_CROSS2'] = CROSS(BIAS['BIAS3'], BIAS['BIAS1'])
    except:
        BIAS = data.data.assign(BIAS1=None,BIAS2=None,BIAS3=None,
                                BIAS_CROSS1=0,BIAS_CROSS2=0)[['BIAS1','BIAS2','BIAS3', 'BIAS_CROSS1','BIAS_CROSS2']]
    try:
        OSC = data.add_func(QA_indicator_OSC)
        OSC['MARK'] = 0
        OSC['OSC_CROSS1'] = CROSS(OSC['OSC'], OSC['MARK'])
        OSC['OSC_CROSS2'] = CROSS(OSC['MARK'], OSC['OSC'])
        OSC['OSC_CROSS3'] = CROSS(OSC['MAOSC'], OSC['MARK'])
        OSC['OSC_CROSS4'] = CROSS(OSC['MARK'], OSC['MAOSC'])
    except:
        OSC = data.data.assign(OSC=None,MAOSC=None,
                               OSC_CROSS1=0,OSC_CROSS2=0,
                               OSC_CROSS3=0,OSC_CROSS4=0)[['OSC','MAOSC','OSC_CROSS1','OSC_CROSS2','OSC_CROSS3','OSC_CROSS4']]
    try:
        ADTM = data.add_func(QA_indicator_ADTM)
        ADTM['ADTM_CROSS1'] = CROSS(ADTM['ADTM'], ADTM['MAADTM'])
        ADTM['ADTM_CROSS2'] = CROSS(ADTM['MAADTM'], ADTM['ADTM'])
    except:
        ADTM = data.data.assign(ADTM=None,MAADTM=None,
                                ADTM_CROSS1=0,ADTM_CROSS2=0,)[['ADTM','MAADTM','ADTM_CROSS1','ADTM_CROSS2']]
    try:
        MACD = data.add_func(QA_indicator_MACD)
        MACD['CROSS_JC'] = CROSS(MACD['DIF'], MACD['DEA'])
        MACD['CROSS_SC'] = CROSS(MACD['DEA'], MACD['DIF'])
        MACD['MACD_TR'] = MACD.apply(lambda x: function1(x.DEA,x.DIF), axis = 1)
    except:
        MACD = data.data.assign(DIF=None,DEA=None,MACD=None,
                                CROSS_JC=0,CROSS_SC=0,)[['DIF','DEA','MACD','CROSS_JC','CROSS_SC','MACD_TR']]
    try:
        DMI = data.add_func(QA_indicator_DMI)
        DMI['ADX_C'] = DMI['ADX']/REF(DMI['ADX'], 1)-1
        DMI['DI_M'] = DMI['DI1'] - DMI['DI2']
        DMI['DI_CROSS1'] = CROSS(DMI['DI1'], DMI['DI2'])
        DMI['DI_CROSS2'] = CROSS(DMI['DI2'], DMI['DI1'])
        DMI['ADX_CROSS1'] = CROSS(DMI['ADX'], DMI['ADXR'])
        DMI['ADX_CROSS2'] = CROSS(DMI['ADXR'], DMI['ADX'])
    except:
        DMI = data.data.assign(DI1=None,DI2=None,ADX=None,ADXR=None,ADX_C=None,DI_M=None,
                               DI_CROSS1=0,DI_CROSS2=0,ADX_CROSS1=0,ADX_CROSS2=0)[['DI1','DI2','ADX','ADXR','ADX_C','DI_M',
                                                                                   'DI_CROSS1','DI_CROSS2','ADX_CROSS1','ADX_CROSS2']]
    try:
        DMA = data.add_func(QA_indicator_DMA)
        DMA['DMA_CROSS1'] = CROSS(DMA['AMA'], DMA['DDD'])
        DMA['DMA_CROSS2'] = CROSS(DMA['DDD'], DMA['AMA'])
    except:
        DMA = data.data.assign(DDD=None,AMA=None,
                               DMA_CROSS1=0,DMA_CROSS2=0)[['DDD','AMA','DMA_CROSS1','DMA_CROSS2']]
    try:
        PBX = data.add_func(QA_indicator_PBX)
        PBX['PBX_STD'] = PBX['PBX1','PBX2','PBX3','PBX4','PBX5','PBX6'].apply(np.std, axis=1)
        PBX['PBX1_C'] = PBX['PBX1']/REF(PBX['PBX1'], 1)-1
        PBX['PBX2_C'] = PBX['PBX2']/REF(PBX['PBX2'], 1)-1
        PBX['PBX3_C'] = PBX['PBX3']/REF(PBX['PBX3'], 1)-1
        PBX['PBX4_C'] = PBX['PBX4']/REF(PBX['PBX4'], 1)-1
        PBX['PBX5_C'] = PBX['PBX5']/REF(PBX['PBX5'], 1)-1
        PBX['PBX6_C'] = PBX['PBX6']/REF(PBX['PBX6'], 1)-1
        #PBX['PBX_TR'] = PBX['PBX1','PBX2','PBX3','PBX4','PBX5','PBX6'].apply(rolling_ols, axis=1)
    except:
        PBX = data.data.assign(PBX1=None,PBX2=None,PBX3=None,PBX4=None,PBX5=None,PBX6=None,
                               PBX1_C=None,PBX2_C=None,PBX3_C=None,PBX4_C=None,PBX5_C=None,PBX6_C=None,
                               PBX_STD=None)[['PBX1','PBX2','PBX3','PBX4','PBX5','PBX6',
                                              'PBX1_C','PBX2_C','PBX3_C','PBX4_C','PBX5_C','PBX6_C',
                                              'PBX_STD']]
    try:
        MTM = data.add_func(QA_indicator_MTM)
        MTM['MARK'] = 0
        MTM['MTM_CROSS1'] = CROSS(MTM['MTM'], MTM['MARK'])
        MTM['MTM_CROSS2'] = CROSS(MTM['MARK'], MTM['MTM'])
        MTM['MTM_CROSS3'] = CROSS(MTM['MTMMA'], MTM['MARK'])
        MTM['MTM_CROSS4'] = CROSS(MTM['MARK'], MTM['MTMMA'])
    except:
        MTM = data.data.assign(MTM=None,MTMMA=None,
                               MTM_CROSS1=0,MTM_CROSS2=0,
                               MTM_CROSS3=0,MTM_CROSS4=0)[['MTM','MTMMA','MTM_CROSS1','MTM_CROSS2','MTM_CROSS3','MTM_CROSS4']]
    try:
        CHO = data.add_func(QA_indicator_CHO)
        CHO['MARK'] = 0
        CHO['CHO_CROSS1'] = CROSS(CHO['CHO'], CHO['CHO'])
        CHO['CHO_CROSS2'] = CROSS(CHO['CHO'], CHO['CHO'])
    except:
        CHO = data.data.assign(CHO=None,MACHO=None)[['CHO','MACHO']]
    try:
        BBI = data.add_func(QA_indicator_BBI)
        BBI['BBI'] = data['close']/BBI['BBI'] - 1
        BBI['BBI_CROSS1'] = CROSS(BBI['BBI'], data['close'])
        BBI['BBI_CROSS2'] = CROSS(data['close'], BBI['BBI'])
    except:
        BBI = data.data.assign(BBI=None,BBI_CROSS1=0,BBI_CROSS2=0)[['BBI','BBI_CROSS1','BBI_CROSS2']]
    try:
        MFI = data.add_func(QA_indicator_MFI)
        MFI['MFI_C'] = MFI['MFI']/REF(MFI['MFI'], 1)-1
    except:
        MFI = data.data.assign(MFI=None,MFI_C=None)[['MFI','MFI_C']]
    try:
        ATR = data.add_func(indicator_ATR)
    except:
        ATR = data.data.assign(TR=None,ATR=None,ATRR=None)[['TR','ATR','ATRR']]
    try:
        SKDJ = data.add_func(QA_indicator_SKDJ)
        SKDJ['SKDJ_CROSS1'] = CROSS(SKDJ['SKDJ_D'], SKDJ['SKDJ_K'])
        SKDJ['SKDJ_CROSS2'] = CROSS(SKDJ['SKDJ_K'], SKDJ['SKDJ_D'])
    except:
        SKDJ = data.data.assign(SKDJ_K=None,SKDJ_D=None,RSV=None,SKDJ_CROSS1=0,
                                SKDJ_CROSS2=0)[['SKDJ_K','SKDJ_D','RSV','SKDJ_CROSS1','SKDJ_CROSS2']]
    try:
        DDI = data.add_func(QA_indicator_DDI)
        DDI['DDI_C'] = DDI['DDI']/REF(DDI['DDI'], 1)-1
        DDI['AD_C'] = DDI['AD']/REF(DDI['AD'], 1)-1
        DDI['ADDI_C'] = DDI['ADDI']/REF(DDI['ADDI'], 1)-1
    except:
        DDI = data.data.assign(DDI=None,ADDI=None,AD=None,DDI_C=None,
                               AD_C=None,ADDI_C=None)[['DDI','ADDI','AD','DDI_C','AD_C','ADDI_C']]
    try:
        shadow = pd.DataFrame(data.add_func(QA_indicator_shadow).values[0])
        shadow.columns=['SHA_LOW','SHA_UP','BODY','BODY_ABS','PRICE_PCG']
    except:
        shadow = data.data.assign(SHA_LOW=None,SHA_UP=None,BODY=None,BODY_ABS=None,PRICE_PCG=None)[['SHA_LOW','SHA_UP','BODY','BODY_ABS','PRICE_PCG']]
    try:
        CDL2CROWS1 = data.add_func(CDL2CROWS)
    except:
        CDL2CROWS1 = data.data.assign(CDL2CROWS=0)['CDL2CROWS']
    try:
        CDL3BLACKCROWS1 = data.add_func(CDL3BLACKCROWS)
    except:
        CDL3BLACKCROWS1 = data.data.assign(CDL3BLACKCROWS=0)['CDL3BLACKCROWS']
    try:
        CDL3INSIDE1 = data.add_func(CDL3INSIDE)
    except:
        CDL3INSIDE1 = data.data.assign(CDL3INSIDE=0)['CDL3INSIDE']
    try:
        CDL3LINESTRIKE1 = data.add_func(CDL3LINESTRIKE)
    except:
        CDL3LINESTRIKE1 = data.data.assign(CDL3LINESTRIKE=0)['CDL3LINESTRIKE']
    try:
        CDL3OUTSIDE1 = data.add_func(CDL3OUTSIDE)
    except:
        CDL3OUTSIDE1 = data.data.assign(CDL3OUTSIDE=0)['CDL3OUTSIDE']
    try:
        CDL3STARSINSOUTH1 = data.add_func(CDL3STARSINSOUTH)
    except:
        CDL3STARSINSOUTH1 = data.data.assign(CDL3STARSINSOUTH=0)['CDL3STARSINSOUTH']
    try:
        CDL3WHITESOLDIERS1 = data.add_func(CDL3WHITESOLDIERS)
    except:
        CDL3WHITESOLDIERS1 = data.data.assign(CDL3WHITESOLDIERS=0)['CDL3WHITESOLDIERS']
    try:
        CDLABANDONEDBABY1 = data.add_func(CDLABANDONEDBABY)
    except:
        CDLABANDONEDBABY1 = data.data.assign(CDLABANDONEDBABY=0)['CDLABANDONEDBABY']
    try:
        CDLADVANCEBLOCK1 = data.add_func(CDLADVANCEBLOCK)
    except:
        CDLADVANCEBLOCK1 = data.data.assign(CDLADVANCEBLOCK=0)['CDLADVANCEBLOCK']
    try:
        CDLBELTHOLD1 = data.add_func(CDLBELTHOLD)
    except:
        CDLBELTHOLD1 = data.data.assign(CDLBELTHOLD=0)['CDLBELTHOLD']
    try:
        CDLBREAKAWAY1 = data.add_func(CDLBREAKAWAY)
    except:
        CDLBREAKAWAY1 = data.data.assign(CDLBREAKAWAY=0)['CDLBREAKAWAY']
    try:
        CDLCLOSINGMARUBOZU1 = data.add_func(CDLCLOSINGMARUBOZU)
    except:
        CDLCLOSINGMARUBOZU1 = data.data.assign(CDLCLOSINGMARUBOZU=0)['CDLCLOSINGMARUBOZU']
    try:
        CDLCONCEALBABYSWALL1 = data.add_func(CDLCONCEALBABYSWALL)
    except:
        CDLCONCEALBABYSWALL1 = data.data.assign(CDLCONCEALBABYSWALL=0)['CDLCONCEALBABYSWALL']
    try:
        CDLCOUNTERATTACK1 = data.add_func(CDLCOUNTERATTACK)
    except:
        CDLCOUNTERATTACK1 = data.data.assign(CDLCOUNTERATTACK=0)['CDLCOUNTERATTACK']
    try:
        CDLDARKCLOUDCOVER1 = data.add_func(CDLDARKCLOUDCOVER)
    except:
        CDLDARKCLOUDCOVER1 = data.data.assign(CDLDARKCLOUDCOVER=0)['CDLDARKCLOUDCOVER']
    try:
        CDLDOJI1 = data.add_func(CDLDOJI)
    except:
        CDLDOJI1 = data.data.assign(CDLDOJI=0)['CDLDOJI']
    try:
        CDLDOJISTAR1 = data.add_func(CDLDOJISTAR)
    except:
        CDLDOJISTAR1 = data.data.assign(CDLDOJISTAR=0)['CDLDOJISTAR']
    try:
        CDLDRAGONFLYDOJI1 = data.add_func(CDLDRAGONFLYDOJI)
    except:
        CDLDRAGONFLYDOJI1 = data.data.assign(CDLDRAGONFLYDOJI=0)['CDLDRAGONFLYDOJI']
    try:
        CDLENGULFING1 = data.add_func(CDLENGULFING)
    except:
        CDLENGULFING1 = data.data.assign(CDLENGULFING=0)['CDLENGULFING']
    try:
        CDLEVENINGDOJISTAR1 = data.add_func(CDLEVENINGDOJISTAR)
    except:
        CDLEVENINGDOJISTAR1 = data.data.assign(CDLEVENINGDOJISTAR=0)['CDLEVENINGDOJISTAR']
    try:
        CDLEVENINGSTAR1 = data.add_func(CDLEVENINGSTAR)
    except:
        CDLEVENINGSTAR1 = data.data.assign(CDLEVENINGSTAR=0)['CDLEVENINGSTAR']
    try:
        CDLGAPSIDESIDEWHITE1 = data.add_func(CDLGAPSIDESIDEWHITE)
    except:
        CDLGAPSIDESIDEWHITE1 = data.data.assign(CDLGAPSIDESIDEWHITE=0)['CDLGAPSIDESIDEWHITE']
    try:
        CDLGRAVESTONEDOJI1 = data.add_func(CDLGRAVESTONEDOJI)
    except:
        CDLGRAVESTONEDOJI1 = data.data.assign(CDLGRAVESTONEDOJI=0)['CDLGRAVESTONEDOJI']
    try:
        CDLHAMMER1 = data.add_func(CDLHAMMER)
    except:
        CDLHAMMER1 = data.data.assign(CDLHAMMER=0)['CDLHAMMER']
    try:
        CDLHANGINGMAN1 = data.add_func(CDLHANGINGMAN)
    except:
        CDLHANGINGMAN1 = data.data.assign(CDLHANGINGMAN=0)['CDLHANGINGMAN']
    try:
        CDLHARAMI1 = data.add_func(CDLHARAMI)
    except:
        CDLHARAMI1 = data.data.assign(CDLHARAMI=0)['CDLHARAMI']
    try:
        CDLHARAMICROSS1 = data.add_func(CDLHARAMICROSS)
    except:
        CDLHARAMICROSS1 = data.data.assign(CDLHARAMICROSS=0)['CDLHARAMICROSS']
    try:
        CDLHIGHWAVE1 = data.add_func(CDLHIGHWAVE)
    except:
        CDLHIGHWAVE1 = data.data.assign(CDLHIGHWAVE=0)['CDLHIGHWAVE']
    try:
        CDLHIKKAKE1 = data.add_func(CDLHIKKAKE)
    except:
        CDLHIKKAKE1 = data.data.assign(CDLHIKKAKE=0)['CDLHIKKAKE']
    try:
        CDLHIKKAKEMOD1 = data.add_func(CDLHIKKAKEMOD)
    except:
        CDLHIKKAKEMOD1 = data.data.assign(CDLHIKKAKEMOD=0)['CDLHIKKAKEMOD']
    try:
        CDLHOMINGPIGEON1 = data.add_func(CDLHOMINGPIGEON)
    except:
        CDLHOMINGPIGEON1 = data.data.assign(CDLHOMINGPIGEON=0)['CDLHOMINGPIGEON']
    try:
        CDLIDENTICAL3CROWS1 = data.add_func(CDLIDENTICAL3CROWS)
    except:
        CDLIDENTICAL3CROWS1 = data.data.assign(CDLIDENTICAL3CROWS=0)['CDLIDENTICAL3CROWS']
    try:
        CDLINNECK1 = data.add_func(CDLINNECK)
    except:
        CDLINNECK1 = data.data.assign(CDLINNECK=0)['CDLINNECK']
    try:
        CDLINVERTEDHAMMER1 = data.add_func(CDLINVERTEDHAMMER)
    except:
        CDLINVERTEDHAMMER1 = data.data.assign(CDLINVERTEDHAMMER=0)['CDLINVERTEDHAMMER']
    try:
        CDLKICKING1 = data.add_func(CDLKICKING)
    except:
        CDLKICKING1 = data.data.assign(CDLKICKING=0)['CDLKICKING']
    try:
        CDLKICKINGBYLENGTH1 = data.add_func(CDLKICKINGBYLENGTH)
    except:
        CDLKICKINGBYLENGTH1 = data.data.assign(CDLKICKINGBYLENGTH=0)['CDLKICKINGBYLENGTH']
    try:
        CDLLADDERBOTTOM1 = data.add_func(CDLLADDERBOTTOM)
    except:
        CDLLADDERBOTTOM1 = data.data.assign(CDLLADDERBOTTOM=0)['CDLLADDERBOTTOM']
    try:
        CDLLONGLEGGEDDOJI1 = data.add_func(CDLLONGLEGGEDDOJI)
    except:
        CDLLONGLEGGEDDOJI1 = data.data.assign(CDLLONGLEGGEDDOJI=0)['CDLLONGLEGGEDDOJI']
    try:
        CDLLONGLINE1 = data.add_func(CDLLONGLINE)
    except:
        CDLLONGLINE1 = data.data.assign(CDLLONGLINE=0)['CDLLONGLINE']
    try:
        CDLMARUBOZU1 = data.add_func(CDLMARUBOZU)
    except:
        CDLMARUBOZU1 = data.data.assign(CDLMARUBOZU=0)['CDLMARUBOZU']
    try:
        CDLMATCHINGLOW1 = data.add_func(CDLMATCHINGLOW)
    except:
        CDLMATCHINGLOW1 = data.data.assign(CDLMATCHINGLOW=0)['CDLMATCHINGLOW']
    try:
        CDLMATHOLD1 = data.add_func(CDLMATHOLD)
    except:
        CDLMATHOLD1 = data.data.assign(CDLMATHOLD=0)['CDLMATHOLD']
    try:
        CDLMORNINGDOJISTAR1 = data.add_func(CDLMORNINGDOJISTAR)
    except:
        CDLMORNINGDOJISTAR1 = data.data.assign(CDLMORNINGDOJISTAR=0)['CDLMORNINGDOJISTAR']
    try:
        CDLMORNINGSTAR1 = data.add_func(CDLMORNINGSTAR)
    except:
        CDLMORNINGSTAR1 = data.data.assign(CDLMORNINGSTAR=0)['CDLMORNINGSTAR']
    try:
        CDLONNECK1 = data.add_func(CDLONNECK)
    except:
        CDLONNECK1 = data.data.assign(CDLONNECK=0)['CDLONNECK']
    try:
        CDLPIERCING1 = data.add_func(CDLPIERCING)
    except:
        CDLPIERCING1 = data.data.assign(CDLPIERCING=0)['CDLPIERCING']
    try:
        CDLRICKSHAWMAN1 = data.add_func(CDLRICKSHAWMAN)
    except:
        CDLRICKSHAWMAN1 = data.data.assign(CDLRICKSHAWMAN=0)['CDLRICKSHAWMAN']
    try:
        CDLRISEFALL3METHODS1 = data.add_func(CDLRISEFALL3METHODS)
    except:
        CDLRISEFALL3METHODS1 = data.data.assign(CDLRISEFALL3METHODS=0)['CDLRISEFALL3METHODS']
    try:
        CDLSEPARATINGLINES1 = data.add_func(CDLSEPARATINGLINES)
    except:
        CDLSEPARATINGLINES1 = data.data.assign(CDLSEPARATINGLINES=0)['CDLSEPARATINGLINES']
    try:
        CDLSHOOTINGSTAR1 = data.add_func(CDLSHOOTINGSTAR)
    except:
        CDLSHOOTINGSTAR1 = data.data.assign(CDLSHOOTINGSTAR=0)['CDLSHOOTINGSTAR']
    try:
        CDLSHORTLINE1 = data.add_func(CDLSHORTLINE)
    except:
        CDLSHORTLINE1 = data.data.assign(CDLSHORTLINE=0)['CDLSHORTLINE']
    try:
        CDLSPINNINGTOP1 = data.add_func(CDLSPINNINGTOP)
    except:
        CDLSPINNINGTOP1 = data.data.assign(CDLSPINNINGTOP=0)['CDLSPINNINGTOP']
    try:
        CDLSTALLEDPATTERN1 = data.add_func(CDLSTALLEDPATTERN)
    except:
        CDLSTALLEDPATTERN1 = data.data.assign(CDLSTALLEDPATTERN=0)['CDLSTALLEDPATTERN']
    try:
        CDLSTICKSANDWICH1 = data.add_func(CDLSTICKSANDWICH)
    except:
        CDLSTICKSANDWICH1 = data.data.assign(CDLSTICKSANDWICH=0)['CDLSTICKSANDWICH']
    try:
        CDLTAKURI1 = data.add_func(CDLTAKURI)
    except:
        CDLTAKURI1 = data.data.assign(CDLTAKURI=0)['CDLTAKURI']
    try:
        CDLTASUKIGAP1 = data.add_func(CDLTASUKIGAP)
    except:
        CDLTASUKIGAP1 = data.data.assign(CDLTASUKIGAP=0)['CDLTASUKIGAP']
    try:
        CDLTHRUSTING1 = data.add_func(CDLTHRUSTING)
    except:
        CDLTHRUSTING1 = data.data.assign(CDLTHRUSTING=0)['CDLTHRUSTING']
    try:
        CDLTRISTAR1 = data.add_func(CDLTRISTAR)
    except:
        CDLTRISTAR1 = data.data.assign(CDLTRISTAR=0)['CDLTRISTAR']
    try:
        CDLUNIQUE3RIVER1 = data.add_func(CDLUNIQUE3RIVER)
    except:
        CDLUNIQUE3RIVER1 = data.data.assign(CDLUNIQUE3RIVER=0)['CDLUNIQUE3RIVER']
    try:
        CDLUPSIDEGAP2CROWS1 = data.add_func(CDLUPSIDEGAP2CROWS)
    except:
        CDLUPSIDEGAP2CROWS1 = data.data.assign(CDLUPSIDEGAP2CROWS=0)['CDLUPSIDEGAP2CROWS']
    try:
        CDLXSIDEGAP3METHODS1 = data.add_func(CDLXSIDEGAP3METHODS)
    except:
        CDLXSIDEGAP3METHODS1 = data.data.assign(CDLXSIDEGAP3METHODS=0)['CDLXSIDEGAP3METHODS']

    res =pd.concat([VR,VRSI,VSTD,BOLL,MIKE,ASI,SAR_V,OBV,PVT,VPT,KDJ,WR,ROC,RSI,CCI,BIAS,OSC,
                    ADTM,MACD,DMI,DMA,PBX,MTM,CHO,BBI,MFI,ATR,SKDJ,DDI,shadow,MA,MA_VOL,
                    CDL2CROWS1,CDL3BLACKCROWS1,CDL3INSIDE1,CDL3LINESTRIKE1,CDL3OUTSIDE1,
                    CDL3STARSINSOUTH1,CDL3WHITESOLDIERS1,CDLABANDONEDBABY1,CDLADVANCEBLOCK1,
                    CDLBELTHOLD1,CDLBREAKAWAY1,CDLCLOSINGMARUBOZU1,CDLCONCEALBABYSWALL1,
                    CDLCOUNTERATTACK1,CDLDARKCLOUDCOVER1,CDLDOJI1,CDLDOJISTAR1,CDLDRAGONFLYDOJI1,
                    CDLENGULFING1,CDLEVENINGDOJISTAR1,CDLEVENINGSTAR1,CDLGAPSIDESIDEWHITE1,
                    CDLGRAVESTONEDOJI1,CDLHAMMER1,CDLHANGINGMAN1,CDLHARAMI1,CDLHARAMICROSS1,
                    CDLHIGHWAVE1,CDLHIKKAKE1,CDLHIKKAKEMOD1,CDLHOMINGPIGEON1,CDLIDENTICAL3CROWS1,
                    CDLINNECK1,CDLINVERTEDHAMMER1,CDLKICKING1,CDLKICKINGBYLENGTH1,CDLLADDERBOTTOM1,
                    CDLLONGLEGGEDDOJI1,CDLLONGLINE1,CDLMARUBOZU1,CDLMATCHINGLOW1,CDLMATHOLD1,
                    CDLMORNINGDOJISTAR1,CDLMORNINGSTAR1,CDLONNECK1,CDLPIERCING1,CDLRICKSHAWMAN1,
                    CDLRISEFALL3METHODS1,CDLSEPARATINGLINES1,CDLSHOOTINGSTAR1,CDLSHORTLINE1,
                    CDLSPINNINGTOP1,CDLSTALLEDPATTERN1,CDLSTICKSANDWICH1,CDLTAKURI1,CDLTASUKIGAP1,
                    CDLTHRUSTING1,CDLTRISTAR1,CDLUNIQUE3RIVER1,CDLUPSIDEGAP2CROWS1,CDLXSIDEGAP3METHODS1],
                   axis=1).dropna(how='all')
    res['SAR_MARK'] = 1 - data['close']/res['SAR']
    res['WR'] = data['close']/res['WR']  - 1
    res['MR'] = data['close']/res['MR'] - 1
    res['SR'] = data['close']/res['SR'] - 1
    res['WS'] = data['close']/res['WS'] - 1
    res['MS'] = data['close']/res['MS'] - 1
    res['SS'] = data['close']/res['SS'] - 1
    res = res.groupby('code').apply(spc)

    res['MA3'] = data['close'] / res['MA3'] - 1
    res['MA5'] = data['close'] / res['MA5'] - 1
    res['MA8'] = data['close'] / res['MA8'] - 1
    res['MA10'] = data['close'] / res['MA10'] - 1
    res['MA12'] = data['close'] / res['MA12'] - 1
    res['MA15'] = data['close'] / res['MA15'] - 1
    res['MA20'] = data['close'] / res['MA20'] - 1
    res['MA30'] = data['close'] / res['MA30'] - 1
    res['MA35'] = data['close'] / res['MA35'] - 1
    res['MA40'] = data['close'] / res['MA40'] - 1
    res['MA45'] = data['close'] / res['MA45'] - 1
    res['MA50'] = data['close'] / res['MA50'] - 1
    res['MA60'] = data['close'] / res['MA60'] - 1

    try:
        vol = data.data['volume']
    except:
        vol = data.data['vol']

    res['MA_VOL3'] = vol / res['MA_VOL3'] - 1
    res['MA_VOL5'] = vol / res['MA_VOL5'] - 1
    res['MA_VOL8'] = vol / res['MA_VOL8'] - 1
    res['MA_VOL10'] = vol / res['MA_VOL10'] - 1
    res['MA_VOL12'] = vol / res['MA_VOL12'] - 1
    res['MA_VOL15'] = vol / res['MA_VOL15'] - 1
    res['MA_VOL20'] = vol / res['MA_VOL20'] - 1
    res['MA_VOL30'] = vol / res['MA_VOL30'] - 1
    res['MA_VOL35'] = vol / res['MA_VOL35'] - 1
    res['MA_VOL40'] = vol / res['MA_VOL40'] - 1
    res['MA_VOL45'] = vol / res['MA_VOL45'] - 1
    res['MA_VOL50'] = vol / res['MA_VOL50'] - 1
    res['MA_VOL60'] = vol / res['MA_VOL60'] - 1

    res['MA5_D'] = res['MA5_C'] - res.groupby('code')['MA5_C'].shift(1)
    res['MA15_D'] = res['MA15_C'] - res.groupby('code')['MA15_C'].shift(1)
    res['MA30_D'] = res['MA30_C'] - res.groupby('code')['MA30_C'].shift(1)
    res['MA60_D'] = res['MA60_C'] - res.groupby('code')['MA60_C'].shift(1)
    res['GMMA3_D'] = res['GMMA3_C'] - res.groupby('code')['GMMA3_C'].shift(1)
    res['GMMA_VOL3_D'] = res['GMMA_VOL3_C'] - res.groupby('code')['GMMA_VOL3_C'].shift(1)
    res['GMMA15_D'] = res['GMMA15_C'] - res.groupby('code')['GMMA15_C'].shift(1)
    res['GMMA_VOL15_D'] = res['GMMA_VOL15_C'] - res.groupby('code')['GMMA_VOL15_C'].shift(1)
    res['GMMA30_D'] = res['GMMA30_C'] - res.groupby('code')['GMMA30_C'].shift(1)
    res['GMMA_VOL30_D'] = res['GMMA_VOL30_C'] - res.groupby('code')['GMMA_VOL30_C'].shift(1)

    if type in ['day','week','month']:
        res = res.reset_index()
        res = res.assign(date=res['date'].apply(lambda x: str(x)[0:10]))
        res = res.set_index(['date','code']).dropna(how='all')
    elif type in ['min','hour']:
        res = res.reset_index()
        res = res.assign(date=res['datetime'].apply(lambda x: str(x)[0:10]))
        res = res.assign(time_stamp=res['datetime'].apply(lambda x: str(x)))
        res = res.set_index(['datetime','code']).dropna(how='all')
    return(res)

def get_indicator_short(data, type='day'):

    #try:
    #    BOLL = data.add_func(QA_indicator_BOLL)
    #    BOLL['WIDTH'] = (BOLL['UB']-BOLL['LB'])/BOLL['BOLL']
    #    BOLL['BOLL'] = data['close'] / BOLL['BOLL'] - 1
    #    BOLL['UB'] = data['close'] / BOLL['UB'] - 1
    #    BOLL['LB'] = data['close'] / BOLL['LB'] - 1
    #except:
    #    BOLL = data.data.assign(BOLL=None,UB=None,LB=None,WIDTH=None,
    #                            BOLL_CROSS1=0,BOLL_CROSS2=0,BOLL_CROSS3=0,
    #                            BOLL_CROSS4=0)[['BOLL','UB','LB','WIDTH']]

    try:
        SKDJ = data.add_func(QA_indicator_SKDJ)
        SKDJ['SKDJ_CROSS1'] = CROSS(SKDJ['SKDJ_D'], SKDJ['SKDJ_K'])
        SKDJ['SKDJ_CROSS2'] = CROSS(SKDJ['SKDJ_K'], SKDJ['SKDJ_D'])
    except:
        SKDJ = data.data.assign(SKDJ_K=None,SKDJ_D=None,RSV=None,SKDJ_CROSS1=0,
                                SKDJ_CROSS2=0)[['SKDJ_K','SKDJ_D','RSV','SKDJ_CROSS1','SKDJ_CROSS2']]

    try:
        SAR_V = data.add_func(SAR,acceleration = 0.02, maximum = 0.2)
    except:
        SAR_V = data.data.assign(SAR=None)[['SAR']]

    try:
        MA = data.add_func(QA_indicator_MA,3,5,8,10,12,15,20,30,35,40,45,50,60)
        MA['SHORT10'] = MA['MA5']/MA['MA10']-1
        MA['SHORT20'] = MA['MA10']/MA['MA20']-1
        MA['SHORT60'] = MA['MA10']/MA['MA60']-1
        MA['LONG60'] = MA['MA20']/MA['MA60']-1
        MA['SHORT_CROSS1'] = CROSS(MA['MA10'], MA['MA20'])
        MA['SHORT_CROSS2'] = CROSS(MA['MA20'], MA['MA10'])
        MA['LONG_CROSS1'] = CROSS(MA['MA20'], MA['MA60'])
        MA['LONG_CROSS2'] = CROSS(MA['MA60'], MA['MA20'])
        MA['LONG_AMOUNT'] = MA['MA20']-MA['MA60']
        MA['SHORT_AMOUNT'] = MA['MA10']-MA['MA20']

        MA['GMMA3'] = MA['MA3']/MA['MA15']-1
        MA['GMMA5'] = MA['MA5']/MA['MA15']-1
        MA['GMMA8'] = MA['MA8']/MA['MA15']-1
        MA['GMMA10'] = MA['MA10']/MA['MA15']-1
        MA['GMMA12'] = MA['MA12']/MA['MA15']-1
        MA['GMMA15'] = MA['MA15']/MA['MA30']-1

        MA['GMMA30'] = MA['MA30']/MA['MA60']-1
        MA['GMMA35'] = MA['MA35']/MA['MA60']-1
        MA['GMMA40'] = MA['MA40']/MA['MA60']-1
        MA['GMMA45'] = MA['MA45']/MA['MA60']-1
        MA['GMMA50'] = MA['MA50']/MA['MA60']-1

    except:
        MA = data.data.assign(MA3=None,MA5=None,MA8=None,MA10=None,MA12=None,MA15=None,MA20=None,
                              MA30=None,MA35=None,MA40=None,MA45=None,MA50=None,MA60=None)[['MA3','MA5','MA8','MA10','MA12','MA15','MA20','MA30','MA35','MA40','MA45','MA50','MA60']]
        MA['SHORT10'] = MA['MA5']/MA['MA10']-1
        MA['SHORT20'] = MA['MA10']/MA['MA20']-1
        MA['SHORT60'] = MA['MA10']/MA['MA60']-1
        MA['LONG60'] = MA['MA20']/MA['MA60']-1
        MA['SHORT_CROSS1'] = CROSS(MA['MA10'], MA['MA20'])
        MA['SHORT_CROSS2'] = CROSS(MA['MA20'], MA['MA10'])
        MA['LONG_CROSS1'] = CROSS(MA['MA20'], MA['MA60'])
        MA['LONG_CROSS2'] = CROSS(MA['MA60'], MA['MA20'])
        MA['LONG_AMOUNT'] = MA['MA20']-MA['MA60']
        MA['SHORT_AMOUNT'] = MA['MA10']-MA['MA20']

        MA['GMMA3'] = MA['MA3']/MA['MA15']-1
        MA['GMMA5'] = MA['MA5']/MA['MA15']-1
        MA['GMMA8'] = MA['MA8']/MA['MA15']-1
        MA['GMMA10'] = MA['MA10']/MA['MA15']-1
        MA['GMMA12'] = MA['MA12']/MA['MA15']-1
        MA['GMMA15'] = MA['MA15']/MA['MA30']-1

        MA['GMMA30'] = MA['MA30']/MA['MA60']-1
        MA['GMMA35'] = MA['MA35']/MA['MA60']-1
        MA['GMMA40'] = MA['MA40']/MA['MA60']-1
        MA['GMMA45'] = MA['MA45']/MA['MA60']-1
        MA['GMMA50'] = MA['MA50']/MA['MA60']-1

    try:
        MA_VOL = data.add_func(QA_indicator_MA_VOL,3,5,8,10,12,15,20,30,35,40,45,50,60)
        MA_VOL['SHORT10V'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL10']-1
        MA_VOL['SHORT20V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL20']-1
        MA_VOL['SHORT60V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL60']-1
        MA_VOL['LONG60V'] = MA_VOL['MA_VOL20']/MA_VOL['MA_VOL60']-1
        MA_VOL['SHORTV_CROSS1'] = CROSS(MA_VOL['MA_VOL10'], MA_VOL['MA_VOL20'])
        MA_VOL['SHORTV_CROSS2'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL10'])
        MA_VOL['LONGV_CROSS1'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL60'])
        MA_VOL['LONGV_CROSS2'] = CROSS(MA_VOL['MA_VOL60'], MA_VOL['MA_VOL20'])
        MA_VOL['LONGV_AMOUNT'] = MA_VOL['MA_VOL20']-MA_VOL['MA_VOL60']
        MA_VOL['SHORTV_AMOUNT'] = MA_VOL['MA_VOL10']-MA_VOL['MA_VOL20']

        MA_VOL['GMMA_VOL3'] = MA_VOL['MA_VOL3']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL5'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL8'] = MA_VOL['MA_VOL8']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL10'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL12'] = MA_VOL['MA_VOL12']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL15'] = MA_VOL['MA_VOL15']/MA_VOL['MA_VOL30']-1

        MA_VOL['GMMA_VOL30'] = MA_VOL['MA_VOL30']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL35'] = MA_VOL['MA_VOL35']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL40'] = MA_VOL['MA_VOL40']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL45'] = MA_VOL['MA_VOL45']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL50'] = MA_VOL['MA_VOL50']/MA_VOL['MA_VOL60']-1

    except:
        MA_VOL = data.data.assign(MA_VOL3=None,MA_VOL5=None,MA_VOL8=None,MA_VOL10=None,MA_VOL12=None,MA_VOL15=None,MA_VOL20=None,
                                  MA_VOL30=None,MA_VOL35=None,MA_VOL40=None,MA_VOL45=None,MA_VOL50=None,MA_VOL60=None)[['MA_VOL3','MA_VOL5','MA_VOL8','MA_VOL10','MA_VOL12','MA_VOL15','MA_VOL20','MA_VOL30','MA_VOL35','MA_VOL40','MA_VOL45','MA_VOL50','MA_VOL60']]
        MA_VOL['SHORT10V'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL10']-1
        MA_VOL['SHORT20V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL20']-1
        MA_VOL['SHORT60V'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL60']-1
        MA_VOL['LONG60V'] = MA_VOL['MA_VOL20']/MA_VOL['MA_VOL60']-1
        MA_VOL['SHORTV_CROSS1'] = CROSS(MA_VOL['MA_VOL10'], MA_VOL['MA_VOL20'])
        MA_VOL['SHORTV_CROSS2'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL10'])
        MA_VOL['LONGV_CROSS1'] = CROSS(MA_VOL['MA_VOL20'], MA_VOL['MA_VOL60'])
        MA_VOL['LONGV_CROSS2'] = CROSS(MA_VOL['MA_VOL60'], MA_VOL['MA_VOL20'])
        MA_VOL['LONGV_AMOUNT'] = MA_VOL['MA_VOL20']-MA_VOL['MA_VOL60']
        MA_VOL['SHORTV_AMOUNT'] = MA_VOL['MA_VOL10']-MA_VOL['MA_VOL20']

        MA_VOL['GMMA_VOL3'] = MA_VOL['MA_VOL3']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL5'] = MA_VOL['MA_VOL5']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL8'] = MA_VOL['MA_VOL8']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL10'] = MA_VOL['MA_VOL10']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL12'] = MA_VOL['MA_VOL12']/MA_VOL['MA_VOL15']-1
        MA_VOL['GMMA_VOL15'] = MA_VOL['MA_VOL15']/MA_VOL['MA_VOL30']-1

        MA_VOL['GMMA_VOL30'] = MA_VOL['MA_VOL30']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL35'] = MA_VOL['MA_VOL35']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL40'] = MA_VOL['MA_VOL40']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL45'] = MA_VOL['MA_VOL45']/MA_VOL['MA_VOL60']-1
        MA_VOL['GMMA_VOL50'] = MA_VOL['MA_VOL50']/MA_VOL['MA_VOL60']-1

    try:
        CCI = data.add_func(QA_indicator_CCI)
        CCI['CCI_CROSS1'] = CROSS(CCI['CCI'], CCI['a'])
        CCI['CCI_CROSS2'] = CROSS(CCI['a'], CCI['CCI'])
        CCI['CCI_CROSS3'] = CROSS(CCI['CCI'], CCI['b'])
        CCI['CCI_CROSS4'] = CROSS(CCI['b'], CCI['CCI'])
    except:
        CCI = data.data.assign(CCI=None,
                               CCI_CROSS1=0,CCI_CROSS2=0,
                               CCI_CROSS3=0,CCI_CROSS4=0)[['CCI','CCI_CROSS1','CCI_CROSS2',
                                                           'CCI_CROSS3','CCI_CROSS4']]

    try:
        MACD = data.add_func(QA_indicator_MACD)
        MACD['CROSS_JC'] = CROSS(MACD['DIF'], MACD['DEA'])
        MACD['CROSS_SC'] = CROSS(MACD['DEA'], MACD['DIF'])
        MACD['MACD_TR'] = MACD.apply(lambda x: function1(x.DEA,x.DIF), axis = 1)
    except:
        MACD = data.data.assign(DIF=None,DEA=None,MACD=None,
                                CROSS_JC=0,CROSS_SC=0,)[['DIF','DEA','MACD','CROSS_JC','CROSS_SC','MACD_TR']]


    res =pd.concat([CCI,MACD,MA,MA_VOL,SKDJ,SAR_V],
                   axis=1).dropna(how='all')
    res = res.groupby('code').apply(spc)
    res['SAR_MARK'] = 1 - data['close']/res['SAR']
    res['MA3'] = data['close'] / res['MA3'] - 1
    res['MA5'] = data['close'] / res['MA5'] - 1
    res['MA8'] = data['close'] / res['MA8'] - 1
    res['MA10'] = data['close'] / res['MA10'] - 1
    #res['MA12'] = data['close'] / res['MA12'] - 1
    #res['MA15'] = data['close'] / res['MA15'] - 1
    #res['MA20'] = data['close'] / res['MA20'] - 1
    #res['MA30'] = data['close'] / res['MA30'] - 1
    #res['MA35'] = data['close'] / res['MA35'] - 1
    #res['MA40'] = data['close'] / res['MA40'] - 1
    #res['MA45'] = data['close'] / res['MA45'] - 1
    #res['MA50'] = data['close'] / res['MA50'] - 1
    #res['MA60'] = data['close'] / res['MA60'] - 1

    #res['MA5_D'] = res['MA5_C'] - res.groupby('code')['MA5_C'].shift(1)
    #res['MA15_D'] = res['MA15_C'] - res.groupby('code')['MA15_C'].shift(1)
    #res['MA30_D'] = res['MA30_C'] - res.groupby('code')['MA30_C'].shift(1)
    #res['MA60_D'] = res['MA60_C'] - res.groupby('code')['MA60_C'].shift(1)
    #res['GMMA3_D'] = res['GMMA3_C'] - res.groupby('code')['GMMA3_C'].shift(1)
    #res['GMMA15_D'] = res['GMMA15_C'] - res.groupby('code')['GMMA15_C'].shift(1)
    #res['GMMA30_D'] = res['GMMA30_C'] - res.groupby('code')['GMMA30_C'].shift(1)

    if type in ['day','week','month']:
        res = res.reset_index()
        res = res.assign(date=res['date'].apply(lambda x: str(x)[0:10]))
        res = res.set_index(['date','code']).dropna(how='all')
    elif type in ['min','hour']:
        res = res.reset_index()
        res = res.assign(date=res['datetime'].apply(lambda x: str(x)[0:10]))
        res = res.assign(time_stamp=res['datetime'].apply(lambda x: str(x)))
        res = res.set_index(['datetime','code']).dropna(how='all')
    return(res)

if __name__ == '__main__':
    pass