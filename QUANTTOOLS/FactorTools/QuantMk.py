from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_stock_target,QA_fetch_get_quant_data,QA_fetch_index_target,
                                           QA_fetch_index_quant_data,QA_fetch_get_index_quant_data,
                                           QA_fetch_stock_quant_pre_adv,QA_fetch_index_quant_pre_adv,
                                           QA_fetch_index_info)
import QUANTAXIS as QA
import pandas as pd

def get_quant_data(start_date, end_date, type = 'crawl', block = False, sub_block= True, method = 'value'):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
    else:
        codes = list(QA.QA_fetch_stock_list_adv()['code'])
        codes = [i for i in codes if i.startswith('688') == False]
        codes = [i for i in codes if i.startswith('787') == False]
        codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, type=None).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_index_quant_data(start_date, end_date, type = 'crawl', method = 'value'):

    codes = QA_fetch_index_info(list(QA.QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)

    if type == 'crawl':
        res = QA_fetch_index_quant_pre_adv(codes,start_date,end_date, method=method).data
    if type == 'model':
        res = QA_fetch_index_quant_data(codes, start_date, end_date)
        target = QA_fetch_index_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    return(pd.get_dummies(res))

def get_index_quant_data_norm(start_date, end_date, type = 'crawl', method = 'value'):

    codes = QA_fetch_index_info(list(QA.QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)

    if type == 'crawl':
        res = QA_fetch_index_quant_pre_adv(codes,start_date,end_date, method=method).data
    if type == 'model':
        res = QA_fetch_get_index_quant_data(codes, start_date, end_date, type='normalization').set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_index_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    return(pd.get_dummies(res))

def get_quant_data_norm(start_date, end_date, type = 'crawl', block = False, sub_block= True, method = 'value'):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
    else:
        codes = list(QA.QA_fetch_stock_list_adv()['code'])
        codes = [i for i in codes if i.startswith('688') == False]
        codes = [i for i in codes if i.startswith('787') == False]
        codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, type='normalization').set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_quant_data_stdd(start_date, end_date, type = 'crawl', block = False, sub_block= True, method = 'value'):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
    else:
        codes = list(QA.QA_fetch_stock_list_adv()['code'])
        codes = [i for i in codes if i.startswith('688') == False]
        codes = [i for i in codes if i.startswith('787') == False]
        codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, type='standardize').set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

if __name__ == 'main':
    pass