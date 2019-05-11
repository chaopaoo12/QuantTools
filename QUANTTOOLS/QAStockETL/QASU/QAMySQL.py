

from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)

from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_financial_report_adv,QA_fetch_stock_financial_calendar_adv,
                                           QA_fetch_stock_divyield_adv,QA_fetch_stock_shares_adv,
                                           QA_fetch_financial_report_wy_adv, QA_fetch_stock_alpha_adv, QA_fetch_stock_technical_index_adv)
from QUANTAXIS.QAFetch.QAQuery import ( QA_fetch_stock_basic_info_tushare, QA_fetch_stock_xdxr)

from QUANTTOOLS.QAStockETL.QAUtil import QA_util_sql_store_mysql
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_process_financial,QA_util_etl_financial_TTM,\
    QA_util_process_stock_financial,QA_util_process_quantdata,QA_util_etl_stock_quant)

import pandas as pd
import datetime


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
    if type == "all":
        data = QA_fetch_stock_day_adv(list(QA_fetch_stock_list_adv()['code'])).data.reset_index()
        QA_util_sql_store_mysql(data, "stock_market_day",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_day_adv(list(QA_fetch_stock_list_adv()['code']), mark_day)
        if data is None:
            print("We have no MARKET data for the day {}".format(mark_day))
        else:
            data = data.data.reset_index()
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
        print("Step Two =================")
        QA_util_process_quantdata(start_date=deal_date)

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
