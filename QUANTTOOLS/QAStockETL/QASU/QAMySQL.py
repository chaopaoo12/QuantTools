

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
from QUANTAXIS.QAUtil import QA_util_log_info
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
    res=data
    res['AVG_TOTAL_MARKET'] =  data['amount']/data['volume']/100
    res[['LAG_MARKET','AVG_LAG_MARKET','LAG_HIGH','LAG_LOW']]= data.shift(1)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['LAG2_MARKET','AVG_LAG2_MARKET']]= data.shift(2)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG3_MARKET','AVG_LAG3_MARKET']]= data.shift(3)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG5_MARKET','AVG_LAG5_MARKET']]= data.shift(5)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG10_MARKET','AVG_LAG10_MARKET']]= data.shift(10)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG20_MARKET','AVG_LAG20_MARKET']]= data.shift(20)[['close_qfq','AVG_TOTAL_MARKET']]
    res[['LAG30_MARKET','AVG_LAG30_MARKET','LAG30_HIGH','LAG30_LOW']]= data.shift(30)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['LAG60_MARKET','AVG_LAG60_MARKET','LAG60_HIGH','LAG60_LOW']]= data.shift(60)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['LAG90_MARKET','AVG_LAG90_MARKET','LAG90_HIGH','LAG90_LOW']]= data.shift(90)[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['AVG10_T_MARKET','AVG10_A_MARKET','HIGH_10','LOW_10']] = data.rolling(window=10).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min'})[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['AVG20_T_MARKET','AVG20_A_MARKET','HIGH_20','LOW_20']] = data.rolling(window=20).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min'})[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['AVG30_T_MARKET','AVG30_A_MARKET','HIGH_30','LOW_30']] = data.rolling(window=30).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min'})[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['AVG60_T_MARKET','AVG60_A_MARKET','HIGH_60','LOW_60']] = data.rolling(window=60).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min'})[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    res[['AVG90_T_MARKET','AVG90_A_MARKET','HIGH_90','LOW_90']] = data.rolling(window=90).agg({'close_qfq':'mean','AVG_TOTAL_MARKET':'mean','high_qfq':'max','low_qfq':'min'})[['close_qfq','AVG_TOTAL_MARKET','high_qfq','low_qfq']]
    a = data.rolling(window=5)
    res[['AVG5_T_MARKET','AVG5_A_MARKET',
         'HIGH_5','LOW_5']] = a.agg({'close_qfq':'mean',
                                     'AVG_TOTAL_MARKET':'mean',
                                     'high_qfq':'max',
                                     'low_qfq':'min'})
    res[[ 'AVG5_C_MARKET','AVG10_C_MARKET',
          'AVG20_C_MARKET','AVG30_C_MARKET',
          'AVG60_C_MARKET','AVG90_C_MARKET']] = a.agg({ 'AVG5_T_MARKET':rolling_ols,
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

    return(res)

#def ETL_stock_day(codes, start=None,end=None):
#    if start is None:
#        data = QA_fetch_stock_day_adv(codes)
#        res1 = data.to_qfq().data
#        res1.columns = [x + '_qfq' for x in res1.columns]
#        data = data.data.join(res1).fillna(0)
#        res = data.groupby('code').apply(pct).reset_index()
#        res = res.where((pd.notnull(res)), None)
#    else:
#        start_date = QA_util_get_pre_trade_date(start,100)
#        data = QA_fetch_stock_day_adv(codes,start_date,end)
#        res1 = data.to_qfq().data
#        res1.columns = [x + '_qfq' for x in res1.columns]
#        data = data.data.join(res1).fillna(0)
#        res = data.groupby('code').apply(pct)
#        res = res.loc[pd.date_range(start, end, freq='D')].reset_index()
#    return(res)

def ETL_stock_day(codes, start=None, end=None):
    if start is None:
        start = '2008-01-01'

    if end is None:
        end = QA_util_today_str()

    if start != end:
        rng = pd.Series(pd.date_range(start, end, freq='D')).apply(lambda x: str(x)[0:10])
    else:
        rng = str(start)[0:10]

    start_date = QA_util_get_pre_trade_date(start,100)
    data = QA_fetch_stock_day_adv(codes,start_date,end)
    res1 = data.to_qfq().data
    res1.columns = [x + '_qfq' for x in res1.columns]
    data = data.data.join(res1).fillna(0).reset_index()
    res = data.groupby('code').apply(pct)
    res = res.reset_index(level = 0,drop = True).reset_index().set_index(['date','code']).loc[rng].replace([np.inf, -np.inf], 0)
    res = res.where((pd.notnull(res)), None)
    return(res)

def QA_etl_stock_list(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK LIST ==== {}'.format(str(datetime.date.today())), ui_log)
    QA_util_sql_store_mysql(QA_fetch_stock_list_adv().reset_index(drop=True), "stock_list",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK LIST HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_shares(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK SHARES ==== {}'.format(str(datetime.date.today())), ui_log)
    data = QA_fetch_stock_shares_adv(list(QA_fetch_stock_list_adv()['code'])).data
    QA_util_sql_store_mysql(data, "stock_shares",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK SHARES HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_info(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK INFO ==== {}'.format(str(datetime.date.today())), ui_log)
    data = pd.DataFrame(QA_fetch_stock_basic_info_tushare())
    data = data.drop("_id", axis=1)
    QA_util_sql_store_mysql(data, "stock_info",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK INFO HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_xdxr(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK XDXR ==== {}'.format(mark_day), ui_log)
    if type == "all":
        data = QA_fetch_stock_xdxr(list(QA_fetch_stock_list_adv()['code'])).reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_xdxr(list(QA_fetch_stock_list_adv()['code']), mark_day)
        if data is None:
            QA_util_log_info("We have no XDXR data for the day {}".format(mark_day))
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK XDXR HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_stock_day(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK DAY ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_stock_list_adv()['code'])
    if type == "all":
        data = ETL_stock_day(codes).reset_index()
        QA_util_sql_store_mysql(data, "stock_market_day",if_exists='replace')
    elif type == "day":
        data = ETL_stock_day(codes, mark_day, mark_day).reset_index()
        if data is None:
            QA_util_log_info("We have no MARKET data for the day {}".format(mark_day))
        else:
            QA_util_sql_store_mysql(data, "stock_market_day",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK DAY HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_stock_financial(type = "crawl", start_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK FINANCIAL REPORT ==== {}'.format(start_date), ui_log)
    if type == 'all':
        data = QA_fetch_financial_report_adv(list(QA_fetch_stock_list_adv()['code'])).data
        columns = [i for i in list(data.columns) if i.startswith('unknown') == False and i.isdigit() == False and i.startswith('IS_R') == False]
        QA_util_sql_store_mysql(data[columns].reset_index(drop=True).fillna(0), "stock_financial",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_adv(list(QA_fetch_stock_list_adv()['code']),start_date,type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK FINANCIAL REPORT HAS BEEN SAVED ==== {}'.format(start_date), ui_log)
        else:
            data = data.reset_index(drop=True).drop("_id",1).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK FINANCIAL REPORT HAS BEEN SAVED ==== {}'.format(start_date), ui_log)

def QA_etl_stock_calendar(type = "crawl", start = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK CALENDAR ==== {}'.format(start), ui_log)
    if type == "all":
        data = QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_list_adv()['code']),start = "all", type = 'report').data.reset_index(drop=True)
        QA_util_sql_store_mysql(data, "stock_calendar",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_list_adv()['code']), start, type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK CALENDAR HAS BEEN SAVED ==== {}'.format(start), ui_log)
        else:
            data = data.reset_index(drop=True)
            QA_util_sql_store_mysql(data, "stock_calendar",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK CALENDAR HAS BEEN SAVED ==== {}'.format(start), ui_log)

def QA_etl_stock_block(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK Block ==== {}'.format(str(datetime.date.today())), ui_log)
    data = QA_fetch_stock_block_adv().data.reset_index()
    QA_util_sql_store_mysql(data, "stock_block",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK Block HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_divyield(type = "crawl", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK divyield ==== {}'.format(mark_day), ui_log)
    if type == "all":
        data = QA_fetch_stock_divyield_adv(list(QA_fetch_stock_list_adv()['code']),start = "all").data.reset_index()
        QA_util_sql_store_mysql(data, "stock_divyield",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_divyield_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK Divyield HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_divyield",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK Divyield HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_process_financial_day(type = "day", deal_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL PROCESS FINANCIAL ==== {}'.format(deal_date), ui_log)
    if type == "day":
        #print("Step One =================")
        QA_util_process_financial(deal_date=deal_date)
        QA_util_log_info(
            '##JOB Now ETL PROCESS FINANCIAL HAS BEEN SAVED ==== {}'.format(deal_date), ui_log)
    elif type == "all":
        QA_util_log_info("Run This JOB in DataBase")

def QA_etl_stock_financial_wy(type = "crawl", start_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK FINANCIAL REPORT WY ==== {}'.format(start_date), ui_log)
    if type == 'all':
        data = QA_fetch_financial_report_wy_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_wy_adv(list(QA_fetch_stock_list_adv()['code']),start_date,type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK FINANCIAL REPORT WY HAS BEEN SAVED ==== {}'.format(start_date), ui_log)
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK FINANCIAL REPORT WY HAS BEEN SAVED ==== {}'.format(start_date), ui_log)

def QA_etl_stock_alpha_day(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK ALPHA ==== {}'.format(mark_day), ui_log)
    if type == "all":
        data = QA_fetch_stock_alpha_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index()
        QA_util_sql_store_mysql(data, "stock_alpha",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_alpha_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK ALPHA HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_alpha",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK ALPHA HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_stock_technical_day(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK TECHNICAL ==== {}'.format(mark_day), ui_log)
    if type == "all":
        data = QA_fetch_stock_technical_index_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index()
        QA_util_sql_store_mysql(data, "stock_technical",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_technical_index_adv(list(QA_fetch_stock_list_adv()['code']), mark_day).data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK TECHNICAL HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_technical",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK TECHNICAL HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)
