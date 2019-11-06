import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_quant_pre_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_target,QA_fetch_get_quant_data
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv

import QUANTAXIS as QA

def get_quant_data(start_date, end_date, type = 'crawl', block = False):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
    else:
        codes = list(QA_fetch_stock_list_adv()['code'])
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date ).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date)
        res = res.join(target)
    res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)