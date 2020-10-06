import numpy as np
from QUANTAXIS import QA_fetch_stock_day_adv,QA_fetch_index_day_adv,QA_fetch_stock_min_adv
from QUANTAXIS.QAUtil import (QA_util_today_str,QA_util_get_pre_trade_date,QA_util_get_trade_range,QA_util_get_real_date,QA_util_if_trade)
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_half_realtime
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_half_adv,QA_fetch_usstock_day_adv
from QUANTTOOLS.QAStockETL.QAUtil.QAAlpha191 import Alpha_191
from QUANTTOOLS.QAStockETL.QAUtil.QAAlpha101 import get_alpha


def stock_alpha(code, date=None):
    np.seterr(invalid='ignore')
    if date == None:
        end_date = QA_util_today_str()
    else:
        end_date = date
    start_date = QA_util_get_pre_trade_date(date, 250)
    try:
        price = QA_fetch_stock_day_adv(code, start_date, end_date).to_qfq().data.reset_index().dropna(axis=0, how='any')
        price = price.assign(volume=price.volume*100)
        price['avg_price'] = price['amount']/price['volume']*price['adj']
        price['prev_close'] = price[['code','close']].groupby('code').shift()
        return(Alpha_191(price, date).alpha())
    except:
        return(None)

def index_alpha(code, date=None):
    np.seterr(invalid='ignore')
    if date == None:
        end_date = QA_util_today_str()
    else:
        end_date = date
    start_date = QA_util_get_pre_trade_date(date, 250)
    try:
        price = QA_fetch_index_day_adv(code, start_date, end_date ).data.reset_index().dropna(axis=0, how='any')
        price = price.assign(volume=price.volume*100)
        price['avg_price'] = price['amount']/price['volume']
        price['prev_close'] = price[['code','close']].groupby('code').shift()
        return(Alpha_191(price, date).alpha())
    except:
        return(None)

def stock_alpha101(code, start=None, end = None):
    np.seterr(invalid='ignore')
    if end is None:
        end_date = QA_util_today_str()
    else:
        end_date = end

    if start is None:
        start = QA_util_today_str()
    else:
        start = start

    start_date = QA_util_get_pre_trade_date(start, 270)
    deal_date_list = QA_util_get_trade_range(start, end)

    try:
        price = QA_fetch_stock_day_adv(code, start_date, end_date ).to_qfq()
        pctchange = price.close_pct_change()
        price = price.data
        price = price.assign(volume=price.volume*100)
        price['avg_price'] = price['amount']/price['volume']*price['adj']
        price['pctchange'] = pctchange
        res = get_alpha(price).reset_index()
        return(res[res.date.isin(deal_date_list)])
    except:
        return(None)

def index_alpha101(code, start=None, end = None):
    np.seterr(invalid='ignore')
    if end is None:
        end_date = QA_util_today_str()
    else:
        end_date = end

    if start is None:
        start = QA_util_today_str()
    else:
        start = start

    start_date = QA_util_get_pre_trade_date(start, 270)
    deal_date_list = QA_util_get_trade_range(start, end)
    try:
        price = QA_fetch_index_day_adv(code, start_date, end_date )
        pctchange = price.close_pct_change()
        price = price.data
        price = price.assign(volume=price.volume*100)
        price['avg_price'] = price['amount']/price['volume']
        price['pctchange'] = pctchange
        res = get_alpha(price).reset_index()
        return(res[res.date.isin(deal_date_list)])
    except:
        return(None)

def stock_alpha101_half(code, start=None, end = None):
    np.seterr(invalid='ignore')
    if end is None:
        end_date = QA_util_today_str()
    else:
        end_date = end

    if start is None:
        start = QA_util_today_str()
    else:
        start = start

    start_date = QA_util_get_pre_trade_date(start, 270)
    deal_date_list = QA_util_get_trade_range(start, end)

    try:
        data = QA_fetch_stock_half_adv(code, start_date, end_date).to_qfq().data
        data['avg_price'] = data['amount']/data['volume']*data['adj']
        price = get_alpha(data).reset_index()
        price = price[price.date.isin(deal_date_list)]

        return(price)
    except:
        return(None)

def stock_alpha101_half_realtime(code, start = None, end = None):
    end =QA_util_today_str()

    if QA_util_if_trade(end):
        pass
    else:
        end = QA_util_get_real_date(end)

    if start is None:
        start = end

    start_date = QA_util_get_pre_trade_date(start, 270)
    deal_date_list = QA_util_get_trade_range(start, end)
    end = QA_util_get_pre_trade_date(end, 1)

    try:
        price = QA_fetch_stock_half_adv(code, start_date, end).to_qfq().data
        price['avg_price'] = price['amount']/price['volume']*price['adj']
        res = QA_fetch_get_stock_half_realtime(code)
        res = res.assign(pctchange=res.close/res.prev_close-1).set_index(['date','code'])[['open','high','low','close','volume','amount','pctchange','avg_price']]
        res = price.append(res)
        res = res.groupby('code').apply(get_alpha)
        res = res.reset_index(level=2).drop('code',axis=1).reset_index()
        return(res[res.date.isin(deal_date_list)])
    except:
        return(None)

def stock_alpha191_half(code, date=None):
    np.seterr(invalid='ignore')
    if date == None:
        end_date = QA_util_today_str()
    else:
        end_date = date
    start_date = QA_util_get_pre_trade_date(date, 250)

    try:
        price = QA_fetch_stock_half_adv(code, start_date, end_date).to_qfq().data.reset_index().dropna(axis=0, how='any')
        price['prev_close'] = price['close']*(1+price['pctchange'])
        price['avg_price'] = price['amount']/price['volume']*price['adj']
        return(Alpha_191(price, date).alpha())
    except:
        return(None)

def stock_alpha191_half_realtime(code, date = None):

    if date == None:
        end_date = QA_util_today_str()
    else:
        end_date = date
    end_date = QA_util_get_pre_trade_date(end_date, 1)
    start_date = QA_util_get_pre_trade_date(date, 250)
    try:
        price = QA_fetch_stock_half_adv(code, start_date, end_date).to_qfq().data.reset_index().dropna(axis=0, how='any')
        price['avg_price'] = price['amount']/price['volume']*price['adj']
        price['prev_close'] = price['close']*(1+price['pctchange'])
        res = QA_fetch_get_stock_half_realtime(code)
        res = price.append(res)
        return(Alpha_191(res, date).alpha())
    except:
        return(None)

def usstock_alpha(code, date=None):
    np.seterr(invalid='ignore')
    if date == None:
        end_date = QA_util_today_str()
    else:
        end_date = date
    start_date = QA_util_get_pre_trade_date(date, 250)
    try:
        price = QA_fetch_usstock_day_adv(code, start_date, end_date).to_qfq().data.reset_index().dropna(axis=0, how='any')
        price['avg_price'] = price['amount']/price['volume']*price['adj']+price['adjust']
        price['prev_close'] = price[['code','close']].groupby('code').shift()
        return(Alpha_191(price, date).alpha())
    except:
        return(None)

def usstock_alpha101(code, start=None, end = None):
    np.seterr(invalid='ignore')
    if end is None:
        end_date = QA_util_today_str()
    else:
        end_date = end

    if start is None:
        start = QA_util_today_str()
    else:
        start = start

    start_date = QA_util_get_pre_trade_date(start, 270)
    deal_date_list = QA_util_get_trade_range(start, end)

    try:
        price = QA_fetch_usstock_day_adv(code, start_date, end_date ).to_qfq()
        pctchange = price.close_pct_change()
        price = price.data
        price['avg_price'] = price['amount']/price['volume']*price['adj']+price['adjust']
        price['pctchange'] = pctchange
        res = get_alpha(price).reset_index()
        return(res[res.date.isin(deal_date_list)])
    except:
        return(None)