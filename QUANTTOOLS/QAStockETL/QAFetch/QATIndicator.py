from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv,QA_fetch_stock_min_adv,QA_fetch_index_day_adv,QA_fetch_index_min_adv,QA_fetch_future_min_adv,QA_fetch_future_day_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_index_day,QA_fetch_index_min
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_half_adv
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator,ohlc,get_indicator_short
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_day,QA_DataStruct_Stock_min,QA_DataStruct_Index_day,QA_DataStruct_Index_min
from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq, QA_fetch_get_stock_min_sina
import numpy as np

def QA_fetch_get_future_indicator(code, start_date, end_date, frequence = 'day'):
    start = QA_util_get_pre_trade_date(start_date,12)
    rng1 = QA_util_get_trade_range(start_date, end_date)
    if frequence == 'day':
        data = QA_fetch_future_day_adv(code,start,end_date)
        data = get_indicator(data, 'day')
    else:
        data = QA_fetch_future_min_adv(code,start+' 00:00:00',end_date + ' 23:59:00',frequence=frequence)
        data = get_indicator(data, 'min')

    data = data[[x for x in list(data.columns) if x not in ['MARK','a','b']]].reset_index()
    data = data[data.date.isin(rng1)]
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_stock_indicator(code, start_date, end_date, type = 'day'):
    if type == 'min':
        start = QA_util_get_pre_trade_date(start_date,12)
        rng1 = QA_util_get_trade_range(start_date, end_date)
        try:
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='30min').to_qfq()
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
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='30min')
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
            data = QA_fetch_stock_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='30min').to_qfq()
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
            data = QA_fetch_index_min_adv(code,start+' 09:30:00',end_date + ' 15:00:00',frequence='30min')
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
    if type == '15min':
        attempts = 0
        success = False
        while attempts < 3 and not success:
            try:
                data = QA_fetch_get_stock_min_sina(code, period='15', type='qfq').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date_stamp'])
                data = data.assign(type='15min',amount=0)
                data = QA_DataStruct_Stock_min(data)
                success = True
            except:
                attempts += 1
                QA_util_log_info("JOB Try {} tims. {code} ======= from {start_date} to {end_date}".format(attempts, code=code, start_date=start_date,end_date=end_date))
                if attempts == 3:
                    QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
                    break
    elif type == '30min':
        attempts = 0
        success = False
        while attempts < 3 and not success:
            try:
                data = QA_fetch_get_stock_min_sina(code, period='30', type='qfq').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date_stamp'])
                data = data.assign(type='30min',amount=0)
                data = QA_DataStruct_Stock_min(data)
            except:
                attempts += 1
                QA_util_log_info("JOB Try {} tims. {code} ======= from {start_date} to {end_date}".format(attempts, code=code, start_date=start_date,end_date=end_date))
                if attempts == 3:
                    QA_util_log_info("JOB No Minly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
                    break
    elif type == 'hour':
        attempts = 0
        success = False
        while attempts < 3 and not success:
            try:
                data = QA_fetch_get_stock_min_sina(code, period='60', type='qfq').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date_stamp'])
                data = data.assign(type='60min',amount=0)
                data = QA_DataStruct_Stock_min(data)
            except:
                attempts += 1
                QA_util_log_info("JOB Try {} tims. {code} ======= from {start_date} to {end_date}".format(attempts, code=code, start_date=start_date,end_date=end_date))
                if attempts == 3:
                    QA_util_log_info("JOB No Hourly data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
                    break
    elif type == 'day':
        start = QA_util_get_pre_trade_date(start_date,80)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_get_usstock_day_xq(code, start, end_date, period='day', type='before').reset_index(drop=True).set_index(['date','code']))
        except:
            QA_util_log_info("JOB No Daily data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))
    elif type == 'week':
        start = QA_util_get_pre_trade_date(start_date,80)
        try:
            data = QA_DataStruct_Stock_day(QA_fetch_get_usstock_day_xq(code, start, end_date, period='week', type='before').reset_index(drop=True).set_index(['date','code']))
            data = QA_DataStruct_Stock_day(data.data.groupby('code',sort=True).apply(ohlc,7))
        except:
            QA_util_log_info("JOB No Week data for {code} ======= from {start_date} to {end_date}".format(code=code, start_date=start_date,end_date=end_date))

    elif type == 'month':
        start = QA_util_get_pre_trade_date(start_date,80)
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
        #if type in ['15min','30min','min','hour']:
        #    data = data[data.date.isin(rng1)]
        #else:
        #    data=data.loc[rng1]
        data = data.assign(SKDJ_TR = (data.SKDJ_CROSS1*-1+ data.SKDJ_CROSS2*1)/(data.SKDJ_CROSS1+data.SKDJ_CROSS2),
                           SHORT_TR = (data.SHORT20 > 0)*1,
                           LONG_TR = (data.LONG60 > 0)*1,
                           TERNS = ((data.SHORT20 > 0) * (data.LONG60 > 0) * (data.LONG_AMOUNT > 0) * 1)
                           )
        data.SKDJ_TR = data.SKDJ_TR.groupby('code').fillna(method='ffill')
        return(data)

if __name__ == '__main__':
    pass