import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_quant_pre_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_target,QA_fetch_get_quant_data
from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
import QUANTAXIS as QA

def get_quant_data(start_date, end_date, type = 'crawl', block = False):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
    else:
        codes = list(QA_fetch_stock_list_adv()['code'])
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date )
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date).set_index(['date','code'])
        target = QA_fetch_stock_target(codes, start_date, end_date)
        res = target.join(res)
    return(res)