

from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)

from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_financial_report_adv,QA_fetch_stock_financial_calendar_adv,
                                           QA_fetch_stock_divyield_adv,QA_fetch_stock_shares_adv,
                                           QA_fetch_financial_report_wy_adv, QA_fetch_stock_alpha_adv, QA_fetch_stock_technical_index_adv)
from QUANTAXIS.QAFetch.QAQuery import ( QA_fetch_stock_basic_info_tushare, QA_fetch_stock_xdxr)

from QUANTTOOLS.QAStockETL.QAUtil import QA_util_sql_store_mysql
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_process_financial,QA_util_etl_financial_TTM,\
    QA_util_process_stock_financial,QA_util_etl_stock_quant)
import numpy as np
import pandas as pd
import datetime

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


def pct(data):
    data['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
    data['LAG_MARKET']= data['close_qfq'].shift(1).apply(lambda x:round(x ,2))
    data['LAG2_MARKET']= data['close_qfq'].shift(2).apply(lambda x:round(x ,2))
    data['LAG3_MARKET']= data['close_qfq'].shift(3).apply(lambda x:round(x ,2))
    data['LAG5_MARKET']= data['close_qfq'].shift(5).apply(lambda x:round(x ,2))
    data['LAG10_MARKET']= data['close_qfq'].shift(10).apply(lambda x:round(x ,2))
    data['LAG20_MARKET']= data['close_qfq'].shift(20).apply(lambda x:round(x ,2))
    data['LAG30_MARKET']= data['close_qfq'].shift(30).apply(lambda x:round(x ,2))
    data['LAG60_MARKET']= data['close_qfq'].shift(60).apply(lambda x:round(x ,2))
    data['LAG90_MARKET']= data['close_qfq'].shift(90).apply(lambda x:round(x ,2))
    data['LAG30_HIGH']= data['high_qfq'].shift(30).apply(lambda x:round(x ,2))
    data['LAG60_HIGH']= data['high_qfq'].shift(60).apply(lambda x:round(x ,2))
    data['LAG90_HIGH']= data['high_qfq'].shift(90).apply(lambda x:round(x ,2))
    data['LAG30_LOW']= data['low_qfq'].shift(30).apply(lambda x:round(x ,2))
    data['LAG60_LOW']= data['low_qfq'].shift(60).apply(lambda x:round(x ,2))
    data['LAG90_LOW']= data['low_qfq'].shift(90).apply(lambda x:round(x ,2))
    data['AVG_LAG_MARKET']= data['AVG_TOTAL_MARKET'].shift(1).apply(lambda x:round(x ,2))
    data['AVG_LAG2_MARKET']= data['AVG_TOTAL_MARKET'].shift(2).apply(lambda x:round(x ,2))
    data['AVG_LAG3_MARKET']= data['AVG_TOTAL_MARKET'].shift(3).apply(lambda x:round(x ,2))
    data['AVG_LAG5_MARKET']= data['AVG_TOTAL_MARKET'].shift(5).apply(lambda x:round(x ,2))
    data['AVG_LAG10_MARKET']= data['AVG_TOTAL_MARKET'].shift(10).apply(lambda x:round(x ,2))
    data['AVG_LAG20_MARKET']= data['AVG_TOTAL_MARKET'].shift(20).apply(lambda x:round(x ,2))
    data['AVG_LAG30_MARKET']= data['AVG_TOTAL_MARKET'].shift(30).apply(lambda x:round(x ,2))
    data['AVG_LAG60_MARKET']= data['AVG_TOTAL_MARKET'].shift(60).apply(lambda x:round(x ,2))
    data['AVG_LAG90_MARKET']= data['AVG_TOTAL_MARKET'].shift(90).apply(lambda x:round(x ,2))
    data['AVG5_T_MARKET']= data['close_qfq'].rolling(window=5).mean().apply(lambda x:round(x ,2))
    data['AVG10_T_MARKET']= data['close_qfq'].rolling(window=10).mean().apply(lambda x:round(x ,2))
    data['AVG20_T_MARKET']= data['close_qfq'].rolling(window=20).mean().apply(lambda x:round(x ,2))
    data['AVG30_T_MARKET']= data['close_qfq'].rolling(window=30).mean().apply(lambda x:round(x ,2))
    data['AVG60_T_MARKET']= data['close_qfq'].rolling(window=60).mean().apply(lambda x:round(x ,2))
    data['AVG90_T_MARKET']= data['close_qfq'].rolling(window=90).mean().apply(lambda x:round(x ,2))
    data['AVG5_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=5).mean().apply(lambda x:round(x ,2))
    data['AVG10_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=10).mean().apply(lambda x:round(x ,2))
    data['AVG20_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=20).mean().apply(lambda x:round(x ,2))
    data['AVG30_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=30).mean().apply(lambda x:round(x ,2))
    data['AVG60_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=60).mean().apply(lambda x:round(x ,2))
    data['AVG90_A_MARKET']= data['AVG_TOTAL_MARKET'].rolling(window=90).mean().apply(lambda x:round(x ,2))
    data['RNG_L']= ((data['high_qfq'].shift(1)-data['low_qfq'].shift(1))/data['low_qfq'].shift(1)).apply(lambda x:round(x * 100 ,2))
    data['RNG_5']= ((data['high_qfq'].rolling(window=5).max()/data['low_qfq'].rolling(window=5).min())-1).apply(lambda x:round(x  ,2))
    data['RNG_10']= ((data['high_qfq'].rolling(window=10).max()/data['low_qfq'].rolling(window=10).min())-1).apply(lambda x:round(x * 100 ,2))
    data['RNG_20']= ((data['high_qfq'].rolling(window=20).max()/data['low_qfq'].rolling(window=20).min())-1).apply(lambda x:round(x * 100 ,2))
    data['RNG_30']= ((data['high_qfq'].rolling(window=30).max()/data['low_qfq'].rolling(window=30).min())-1).apply(lambda x:round(x * 100 ,2))
    data['RNG_60']= ((data['high_qfq'].rolling(window=60).max()/data['low_qfq'].rolling(window=60).min())-1).apply(lambda x:round(x * 100 ,2))
    data['RNG_90']= ((data['high_qfq'].rolling(window=90).max()/data['low_qfq'].rolling(window=60).min())-1).apply(lambda x:round(x * 100 ,2))
    data['AVG5_C_MARKET']= data['AVG5_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    data['AVG10_C_MARKET']= data['AVG10_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    data['AVG20_C_MARKET']= data['AVG20_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    data['AVG30_C_MARKET']= data['AVG30_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    data['AVG60_C_MARKET']= data['AVG60_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    data['AVG90_C_MARKET']= data['AVG90_T_MARKET'].rolling(window=5).apply(rolling_ols,raw=True)
    return(data)

def ETL_stock_day(codes, start=None,end=None):
    if start is None:
        data = QA_fetch_stock_day_adv(codes)
        res1 = data.to_qfq().data
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data.join(res1).fillna(0).reset_index()
        res = data.groupby('code').apply(pct).reset_index(drop = True).set_index(['date','code'])
        res = res.where((pd.notnull(res)), None)
    else:
        start_date = QA_util_get_pre_trade_date(start,100)
        data = QA_fetch_stock_day_adv(codes,start_date,end)
        res1 = data.to_qfq().data
        res1.columns = [x + '_qfq' for x in res1.columns]
        data = data.data.join(res1).fillna(0).reset_index()
        res = data.groupby('code').apply(pct)
        res = res.reset_index(drop = True).set_index(['date','code']).loc[pd.date_range(start, end, freq='D')]
    return(res)

def QA_etl_stock_list():
    QA_util_sql_store_mysql(QA_fetch_stock_list_adv().reset_index(drop=True), "stock_list",if_exists='replace')

def QA_etl_stock_shares():
    data = QA_fetch_stock_shares_adv(list(QA_fetch_stock_list_adv()['code'])).data
    QA_util_sql_store_mysql(data, "stock_shares",if_exists='replace')

def QA_etl_stock_info():
    data = pd.DataFrame(QA_fetch_stock_basic_info_tushare())
    data = data.drop("_id", axis=1)
    QA_util_sql_store_mysql(data, "stock_info",if_exists='replace')

def QA_etl_stock_xdxr(type = "day", mark_day = str(datetime.date.today())):
    if type == "all":
        data = QA_fetch_stock_xdxr(list(QA_fetch_stock_list_adv()['code'])).reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_xdxr(list(QA_fetch_stock_list_adv()['code']), mark_day)
        if data is None:
            print("We have no XDXR data for the day {}".format(mark_day))
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='append')

def QA_etl_stock_day(type = "day", mark_day = str(datetime.date.today())):
    codes = list(QA_fetch_stock_list_adv()['code'])
    if type == "all":
        data = ETL_stock_day(codes).reset_index()
        QA_util_sql_store_mysql(data, "stock_market_day",if_exists='replace')
    elif type == "day":
        data = ETL_stock_day(codes, mark_day, mark_day).reset_index()
        if data is None:
            print("We have no MARKET data for the day {}".format(mark_day))
        else:
            QA_util_sql_store_mysql(data, "stock_market_day",if_exists='append')

def QA_etl_stock_financial(type = "crawl", start_date = str(datetime.date.today())):
    if type == 'all':
        data = QA_fetch_financial_report_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_financial",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_adv(list(QA_fetch_stock_list_adv()['code']),start_date,type = 'crawl').data
        print(data)
        if data is None:
            print("We have no financial data for the day {}".format(start_date))
        else:
            data = data.reset_index(drop=True).drop("_id",1).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial",if_exists='append')

def QA_etl_stock_calendar(type = "crawl", start = str(datetime.date.today())):
    if type == "all":
        data = QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_list_adv()['code']),start = "all", type = 'report').data.reset_index(drop=True)
        QA_util_sql_store_mysql(data, "stock_calendar",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_list_adv()['code']), start, type = 'crawl').data
        if data is None:
            print("We have no calendar data for the day {}".format(start))
        else:
            data = data.reset_index(drop=True)
            QA_util_sql_store_mysql(data, "stock_calendar",if_exists='append')

def QA_etl_stock_block():
    data = QA_fetch_stock_block_adv().data.reset_index()
    QA_util_sql_store_mysql(data, "stock_block",if_exists='replace')

def QA_etl_stock_divyield(type = "crawl", mark_day = str(datetime.date.today())):
    if type == "all":
        data = QA_fetch_stock_divyield_adv(list(QA_fetch_stock_list_adv()['code']),start = "all").data.reset_index()
        QA_util_sql_store_mysql(data, "stock_divyield",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_divyield_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            print("We have no Divyield data for the day {}".format(mark_day))
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_divyield",if_exists='append')

def QA_etl_process_financial_day(type = "day", deal_date = str(datetime.date.today())):
    if type == "day":
        print("Step One =================")
        QA_util_process_financial(deal_date=deal_date)

    elif type == "all":
        print("Run This JOB in DataBase")

def QA_etl_stock_financial_wy(type = "crawl", start_date = str(datetime.date.today())):
    if type == 'all':
        data = QA_fetch_financial_report_wy_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_wy_adv(list(QA_fetch_stock_list_adv()['code']),start_date,type = 'crawl').data
        if data is None:
            print("We have no financial data for the day {}".format(str(datetime.date.today())))
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='append')

def QA_etl_stock_alpha_day(type = "day", mark_day = str(datetime.date.today())):
    if type == "all":
        data = QA_fetch_stock_alpha_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index()
        QA_util_sql_store_mysql(data, "stock_alpha",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_alpha_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            print("We have no Alpha data for the day {}".format(mark_day))
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_alpha",if_exists='append')

def QA_etl_stock_technical_day(type = "day", mark_day = str(datetime.date.today())):
    if type == "all":
        data = QA_fetch_stock_technical_index_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index()
        QA_util_sql_store_mysql(data, "stock_technical",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_technical_index_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            print("We have no Technical data for the day {}".format(mark_day))
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_technical",if_exists='append')
