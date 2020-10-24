import numpy as np

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
        data['AVG_TARGET'] = (data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET']-1).apply(lambda x:round(x * 100,2))
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = (data['PRE2_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET3'] = (data['PRE3_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET4'] = (data['PRE4_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET5'] = (data['PRE5_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
        data['TARGET10'] = (data['PRE10_MARKET']/data['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
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
    market['INDEX_TARGET'] = (market['PRE2_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET3'] = (market['PRE3_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET4'] = (market['PRE4_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET5'] = (market['PRE5_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
    market['INDEX_TARGET10'] = (market['PRE10_MARKET']/market['PRE_MARKET']-1).apply(lambda x:round(x * 100,2))
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
        data['AVG_TARGET'] = np.log(data['AVG_PRE_MARKET']/data['AVG_TOTAL_MARKET'])
    elif type == 'high':
        data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
        data[['PRE_MARKET','AVG_PRE_MARKET']]= data.shift(-1)[['close_qfq','AVG_TOTAL_MARKET']]
        data[['PRE2_MARKET','AVG_PRE2_MARKET']]= data.shift(-2)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE3_MARKET','AVG_PRE3_MARKET']]= data.shift(-3)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE4_MARKET','AVG_PRE4_MARKET']]= data.shift(-4)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE5_MARKET','AVG_PRE5_MARKET']]= data.shift(-5)[['high_qfq','AVG_TOTAL_MARKET']]
        data[['PRE10_MARKET','AVG_PRE10_MARKET']]= data.shift(-10)[['high_qfq','AVG_TOTAL_MARKET']]
        data['TARGET'] = np.log(data['PRE2_MARKET']/data['PRE_MARKET'])
        data['TARGET3'] = np.log(data['PRE3_MARKET']/data['PRE_MARKET'])
        data['TARGET4'] = np.log(data['PRE4_MARKET']/data['PRE_MARKET'])
        data['TARGET5'] = np.log(data['PRE5_MARKET']/data['PRE_MARKET'])
        data['TARGET10'] = np.log(data['PRE10_MARKET']/data['PRE_MARKET'])
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
