from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv,QA_fetch_stock_min_adv,QA_fetch_index_day_adv,QA_fetch_index_min_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_index_day,QA_fetch_index_min
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_half_adv
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator,ohlc,get_indicator_short
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq
import numpy as np

def QA_fetch_get_stock_indicator(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,12)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='15min').to_qfq()
        except:
            QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'hour':
        start = QA_util_get_pre_trade_date(start_date,55)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='60min').to_qfq()
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_day_adv(code,start,end_date).to_qfq()
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_stock_day_adv(code,start,end_date).to_qfq().week)
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,220)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_stock_day_adv(code,start,end_date).to_qfq().month)
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    if data is None:
        return None
    else:
        data = get_indicator(data, type)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_index_indicator(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,12)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='15min')
        except:
            QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'hour':
        start = QA_util_get_pre_trade_date(start_date,55)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='60min')
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,180)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_day_adv(code,start,end_date)
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,187)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Index_day(QA_fetch_index_day_adv(code,start,end_date).week)
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,210)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Index_day(QA_fetch_index_day_adv(code,start,end_date).month)
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    if data is None:
        return None
    else:
        data = get_indicator(data, type)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_stock_indicator_half(code, start_date, end_date, type = 'day'):
    if type == 'day':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date)
            data = data.to_qfq()
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date).to_qfq()
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,7))
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,220)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date).to_qfq()
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,30))
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
            data = None
    if data is None:
        return None
    else:
        data = get_indicator(data)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_stock_indicator_halfreal(code, start_date, end_date, type = 'day'):
    if type == 'day':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date)
            data = data.to_qfq()
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date)
            data = data.to_qfq()
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,7))
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,220)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_half_adv(code,start,end_date)
            data = data.to_qfq()
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,30))
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    if data is None:
        return None
    else:
        data = get_indicator(data)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_stock_indicator_short(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,12)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='15min').to_qfq()
        except:
            QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'hour':
        start = QA_util_get_pre_trade_date(start_date,55)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='60min').to_qfq()
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_day_adv(code,start,end_date).to_qfq()
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,200)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_stock_day_adv(code,start,end_date).to_qfq().week)
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,220)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_stock_day_adv(code,start,end_date).to_qfq().month)
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    if data is None:
        return None
    else:
        data = get_indicator_short(data, type)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_index_indicator_short(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,12)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='15min')
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'hour':
        start = QA_util_get_pre_trade_date(start_date,55)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='60min')
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_index_day_adv(code,start,end_date)
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Index_day(QA_fetch_index_day_adv(code,start,end_date).week)
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
            data=None

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Index_day(QA_fetch_index_day_adv(code,start,end_date).month)
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
            data = None

    if data is None:
        return None
    else:
        data = get_indicator_short(data, type)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
        data = data[data.date.isin(rng1)]
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)

def QA_fetch_get_stock_indicator_realtime(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,20)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_get_usstock_day_xq(code, start, end_date, period='15m', type='before').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date','date_stamp'])
            data['type'] = '15min'
            data = QA_DataStruct_Stock_min(data)
        except:
            QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'hour':
        start = QA_util_get_pre_trade_date(start_date,55)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_get_usstock_day_xq(code, start, end_date, period='60m', type='before').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date','date_stamp'])
            data['type'] = '60min'
            data = QA_DataStruct_Stock_min(data)
        except:
            QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_get_usstock_day_xq(code, start, end_date, period='day', type='before').reset_index(drop=True).set_index(['date','code']))
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_get_usstock_day_xq(code, start, end_date, period='week', type='before').reset_index(drop=True).set_index(['date','code']))
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,7))
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,80)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_get_usstock_day_xq(code, start, end_date, period='month', type='before').reset_index(drop=True).set_index(['date','code'])).to_qfq()
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,30))
        except:
            QA_util_log_info("JOB No Month data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    if data is None:
        return None
    else:
        data = get_indicator(data, type)
        data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]]
        if type in ['min','hour']:
            data = data[data.date.isin(rng1)]
        else:
            data=data.loc[rng1]
        data['SKDJ_TR'] = data.apply(lambda x: (x.SKDJ_K > x.SKDJ_D) * 1, axis=1)
        if type == 'min':
            data['TERNS'] = data.apply(lambda x: (x.SHORT20 > 0) * (x.LONG60 > 0) * 1, axis=1)
        else:
            data['TERNS'] = data.apply(lambda x: (x.SHORT20 > 0) * (x.LONG60 > 0) * (x.LONG_AMOUNT > 0) * 1, axis=1)
        return(data)