import numpy as np
import pandas as pd
from scipy import stats

def rolling_ols(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #slope = np.diff(y)/np.diff(pd.Series(range(1,len(y)+1)))
    #return(slope)
    model = stats.linregress(pd.Series(range(1,len(y)+1)),y)
    return(round(model.slope,2))

def rolling_count1(data):
    return(data[data< 0].count())

def rolling_count2(data):
    return(data[data> 0].count())

def rolling_mean1(data):
    return(data[data< 0].mean())

def rolling_mean2(data):
    return(data[data> 0].mean())

def uspct(data):
    res=data
    res[['LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW','LAG_AMOUNT']]= data.shift(1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq','amount']]
    res['returns'] = res['close_qfq']/res['LAG_MARKET'] - 1
    res[['LAG2_MARKET','AVG_LAG2_MARKET']]= data.shift(2)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG3_MARKET','AVG_LAG3_MARKET']]= data.shift(3)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG5_MARKET','AVG_LAG5_MARKET']]= data.shift(5)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG10_MARKET','AVG_LAG10_MARKET']]= data.shift(10)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG20_MARKET','AVG_LAG20_MARKET']]= data.shift(20)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW']]= data.shift(30)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW']]= data.shift(60)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW']]= data.shift(90)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]

    res[['AVG5_T_MARKET','AVG5_A_MARKET','HIGH_5','LOW_5','AMOUNT_5','MAMOUNT_5','NEGRT_CNT5','POSRT_CNT5','NEGRT_MEAN5','POSRT_MEAN5']] = res.rolling(window=5).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})
    res[['AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10','AMOUNT_10','MAMOUNT_10','NEGRT_CNT10','POSRT_CNT10','NEGRT_MEAN10','POSRT_MEAN10']] = res.rolling(window=10).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})
    res[['AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20','AMOUNT_20','MAMOUNT_20','NEGRT_CNT20','POSRT_CNT20','NEGRT_MEAN20','POSRT_MEAN20']] = res.rolling(window=20).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})
    res[['AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30','AMOUNT_30','MAMOUNT_30','NEGRT_CNT30','POSRT_CNT30','NEGRT_MEAN30','POSRT_MEAN30']] = res.rolling(window=30).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})
    res[['AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60','AMOUNT_60','MAMOUNT_60','NEGRT_CNT60','POSRT_CNT60','NEGRT_MEAN60','POSRT_MEAN60']] = res.rolling(window=60).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})
    res[['AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90','AMOUNT_90','MAMOUNT_90','NEGRT_CNT90','POSRT_CNT90','NEGRT_MEAN90','POSRT_MEAN90']] = res.rolling(window=90).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min','amount':['mean','max'],'returns':[rolling_count1,rolling_count2,rolling_mean1,rolling_mean2]})

    res[[ 'AVG5_C_MARKET','AVG10_C_MARKET',
          'AVG20_C_MARKET','AVG30_C_MARKET',
          'AVG60_C_MARKET','AVG90_C_MARKET']] = res.rolling(window=5).agg({ 'AVG5_T_MARKET':rolling_ols,
                                                                            'AVG10_T_MARKET':rolling_ols,
                                                                            'AVG20_T_MARKET':rolling_ols,
                                                                            'AVG30_T_MARKET':rolling_ols,
                                                                            'AVG60_T_MARKET':rolling_ols,
                                                                            'AVG90_T_MARKET':rolling_ols})

    res['RNG_L']= (res['LAG_HIGH']/res['LAG_LOW']-1).apply(lambda x:round(x ,4))
    res['RNG_5']= (res['HIGH_5']/res['LOW_5']-1).apply(lambda x:round(x ,4))
    res['RNG_10']= (res['HIGH_10']/res['LOW_10']-1).apply(lambda x:round(x ,4))
    res['RNG_20']= (res['HIGH_20']/res['LOW_20']-1).apply(lambda x:round(x ,4))
    res['RNG_30']= (res['HIGH_30']/res['LOW_30']-1).apply(lambda x:round(x ,4))
    res['RNG_60']= (res['HIGH_60']/res['LOW_60']-1).apply(lambda x:round(x ,4))
    res['RNG_90']= (res['HIGH_90']/res['LOW_90']-1).apply(lambda x:round(x ,4))
    res['AMT_L']= (res['amount']/res['LAG_AMOUNT']-1).apply(lambda x:round(x ,4))
    res['AMT_5']= (res['amount']/res['AMOUNT_5']-1).apply(lambda x:round(x ,4))
    res['AMT_10']= (res['amount']/res['AMOUNT_10']-1).apply(lambda x:round(x ,4))
    res['AMT_20']= (res['amount']/res['AMOUNT_20']-1).apply(lambda x:round(x ,4))
    res['AMT_30']= (res['amount']/res['AMOUNT_30']-1).apply(lambda x:round(x ,4))
    res['AMT_60']= (res['amount']/res['AMOUNT_60']-1).apply(lambda x:round(x ,4))
    res['AMT_90']= (res['amount']/res['AMOUNT_90']-1).apply(lambda x:round(x ,4))
    res['MAMT_5']= (res['amount']/res['MAMOUNT_5']-1).apply(lambda x:round(x ,4))
    res['MAMT_10']= (res['amount']/res['MAMOUNT_10']-1).apply(lambda x:round(x ,4))
    res['MAMT_20']= (res['amount']/res['MAMOUNT_20']-1).apply(lambda x:round(x ,4))
    res['MAMT_30']= (res['amount']/res['MAMOUNT_30']-1).apply(lambda x:round(x ,4))
    res['MAMT_60']= (res['amount']/res['MAMOUNT_60']-1).apply(lambda x:round(x ,4))
    res['MAMT_90']= (res['amount']/res['MAMOUNT_90']-1).apply(lambda x:round(x ,4))
    return(res)

def pct(data, type = 'close'):
    data = data.sort_values('date',ascending=True)
    if type == 'close':
        #data = data.assign(up_rate= lambda x: 0.2 if x.date >= '2020-08-24' and str(x.code).startswith('300') == True else 0.1)
        data['up_rate'] = data.apply(lambda x : 0.2 if str(x['date']) >= '2020-08-24'and str(x.code).startswith('300') == True else 0.1,axis=1)
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET','high_mark','low_mark']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
        data[['PRE_DATE','PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['date','close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE20_MARKET','AVG_PRE20_MARKET']]= data.shift(-20)[['close_qfq','AVG_TOTAL_MARKET']]
        data['OPEN_MARK'] = (data['high_mark'] == data['low_mark']) * 1
        data['UP_PRICE'] = data['close_qfq'] + (data['close_qfq'] * data['up_rate']).apply(lambda x:round(x,2))
        data['DW_PRICE'] = data['close_qfq'] - (data['close_qfq'] * data['up_rate']).apply(lambda x:round(x,2))
        data['OPEN_MARK'] = ((data['PRE_MARKET'] <= data['DW_PRICE']) | (data['PRE_MARKET'] >= data['UP_PRICE']) | (data['OPEN_MARK'] == 1))*1
        data['PASS_MARK'] = (data['PRE_MARKET']/data['close_qfq']-1).apply(lambda x:round(x * 100,2))
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET4'] = (data['PRE4_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET10'] = (data['PRE10_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET20'] = (data['PRE20_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = (data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET']-1).apply(lambda x:round(x * 100,2))
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE20_MARKET','AVG_PRE20_MARKET']]= data.shift(-20)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET4'] = (data['PRE4_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET10'] = (data['PRE10_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET20'] = (data['PRE20_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['AVG_TARGET'] = (data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET']-1).apply(lambda x:round(x * 100,2))
    else:
        data=None
    return(data)

def index_pct(market):
    market = market.sort_values('date',ascending=True)
    market['PRE_MARKET']= market.shift(-1)['close']
    market['PRE2_MARKET']= market.shift(-2)['close']
    market['PRE3_MARKET']= market.shift(-3)['close']
    market['PRE4_MARKET']= market.shift(-4)['close']
    market['PRE5_MARKET']= market.shift(-5)['close']
    market['PRE10_MARKET']= market.shift(-10)['close']
    market['PRE20_MARKET']= market.shift(-20)['close']
    market['INDEX_TARGET'] = (market['PRE2_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET3'] = (market['PRE3_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET4'] = (market['PRE4_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET5'] = (market['PRE5_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET10'] = (market['PRE10_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET20'] = (market['PRE20_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    return(market)

def pct_log(data, type = 'close'):
    data = data.sort_values('date',ascending=True)
    if type == 'close':
        data = data.assign(up_rate= lambda x: 0.2 if (x.date >= '2020-08-24') and (str(x.code).startswith('300') == True) else 0.1)
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET','high_mark','low_mark']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
        data[['PRE_DATE','PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['date','close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE20_MARKET','AVG_PRE20_MARKET']]= data.shift(-20)[['close_qfq','AVG_TOTAL_MARKET']]
        data['OPEN_MARK'] = (data['high_mark'] == data['low_mark']) * 1
        data['UP_PRICE'] = data['close_qfq'] + (data['close_qfq'] * data['up_rate']).apply(lambda x:round(x,2))
        data['DW_PRICE'] = data['close_qfq'] - (data['close_qfq'] * data['up_rate']).apply(lambda x:round(x,2))
        data['OPEN_MARK'] = ((data['PRE_MARKET'] <= data['DW_PRICE']) | (data['PRE_MARKET'] >= data['UP_PRICE']) | (data['OPEN_MARK'] == 1))*1
        data['PASS_MARK'] = (data['PRE_MARKET']/data['close_qfq']-1).apply(lambda x:round(x * 100,2))
        data['TARGET'] = np.log(data['PRE2_MARKET']/data['PRE_MARKET'])
        data['TARGET3'] = np.log(data['PRE3_MARKET']/data['PRE_MARKET'])
        data['TARGET4'] = np.log(data['PRE4_MARKET']/data['PRE_MARKET'])
        data['TARGET5'] = np.log(data['PRE5_MARKET']/data['PRE_MARKET'])
        data['TARGET10'] = np.log(data['PRE10_MARKET']/data['PRE_MARKET'])
        data['TARGET20'] = np.log(data['PRE20_MARKET']/data['PRE_MARKET'])
        data['AVG_TARGET'] = np.log(data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET'])
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE20_MARKET','AVG_PRE20_MARKET']]= data.shift(-20)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = np.log(data['PRE2_MARKET']/data['PRE_MARKET'])
        data['TARGET3'] = np.log(data['PRE3_MARKET']/data['PRE_MARKET'])
        data['TARGET4'] = np.log(data['PRE4_MARKET']/data['PRE_MARKET'])
        data['TARGET5'] = np.log(data['PRE5_MARKET']/data['PRE_MARKET'])
        data['TARGET10'] = np.log(data['PRE10_MARKET']/data['PRE_MARKET'])
        data['TARGET20'] = np.log(data['PRE20_MARKET']/data['PRE_MARKET'])
        data['AVG_TARGET'] = np.log(data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET'])
    else:
        data=None
    return(data)

def index_pct_log(market):
    market = market.sort_values('date',ascending=True)
    market['PRE_MARKET']= market.shift(-1)['close']
    market['PRE2_MARKET']= market.shift(-2)['close']
    market['PRE3_MARKET']= market.shift(-3)['close']
    market['PRE4_MARKET']= market.shift(-4)['close']
    market['PRE5_MARKET']= market.shift(-5)['close']
    market['PRE10_MARKET']= market.shift(-10)['close']
    market['INDEX_TARGET'] = np.log(market['PRE2_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET3'] = np.log(market['PRE3_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET4'] = np.log(market['PRE4_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET5'] = np.log(market['PRE5_MARKET']/market['PRE_MARKET'])
    market['INDEX_TARGET10'] = np.log(market['PRE10_MARKET']/market['PRE_MARKET'])
    return(market)
