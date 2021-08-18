from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq, QA_fetch_get_stock_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range

def QA_fetch_get_stock_vwap(code, start_date, end_date, period = '1'):
    try:
        QA_util_log_info("JOB Get {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = QA_fetch_get_stock_min_sina(code, period=period, type='qfq').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date_stamp'])
        data = data.assign(date=data.reset_index().datetime.apply(lambda x:str(x)[0:10]).tolist(),
                           amt=((data['high'] +data['low']) / 2) * data['volume'])

        data = data.assign(camt = data.groupby('date')['amt'].cumsum(),
                           cvolume = data.groupby('date')['volume'].cumsum())
        data['vamp'] = data['camt'] / data['cvolume']
    except:
        QA_util_log_info("JOB No {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = None

    return(data)
