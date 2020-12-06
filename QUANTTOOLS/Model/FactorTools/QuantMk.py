from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_stock_target,QA_fetch_get_quant_data,QA_fetch_index_target,
                                           QA_fetch_index_quant_data,QA_fetch_get_index_quant_data,
                                           QA_fetch_stock_quant_pre_adv,QA_fetch_index_quant_pre_adv,QA_fetch_stock_all,
                                           QA_fetch_index_info,QA_fetch_stock_om_all,QA_fetch_stock_quant_pre_train_adv,
                                           QA_fetch_get_quant_data_train,QA_fetch_get_quant_data_realtime,
                                           QA_fetch_code_new,QA_fetch_index_quant_hour,QA_fetch_index_hour_pre,QA_fetch_stock_quant_hour,QA_fetch_stock_hour_pre)
import QUANTAXIS as QA
from QUANTAXIS.QAUtil import QA_util_log_info
import pandas as pd

def get_quant_data_train(start_date, end_date, code=None, type = 'crawl', block = False, sub_block= True, method = 'value', norm_type = 'normalization'):
    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_train_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data_train(codes, start_date, end_date, norm_type=norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_quant_data_realtime(start_date, end_date, code=None, type = 'model', block = False, sub_block= True, method = 'value', norm_type = 'normalization'):

    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_train_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data_realtime(codes, start_date, end_date, norm_type =norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_index_quant_data(start_date, end_date, code=None, type = 'crawl', method = 'value',norm_type=None):

    code_list = QA.QA_fetch_index_list_adv()

    if code is None:
        code_list = QA_fetch_index_info(list(code_list.code))
        codes = list(code_list[code_list.cate != '5'].code)
    else:
        codes = list(code_list[code_list.code.isin(code)].code)
    codes = codes + ['000001','399001','399006']

    codes = [i for i in codes if i not in ['880602','880604', '880650', '880608']]

    if type == 'crawl':
        res = QA_fetch_index_quant_pre_adv(codes,start_date,end_date, method=method,norm_type=norm_type).data
    if type == 'model':
        res = QA_fetch_index_quant_data(codes, start_date, end_date)
        target = QA_fetch_index_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    return(res)

def get_index_quant_data_norm(start_date, end_date, code=None, type = 'crawl', method = 'value',norm_type=None):

    code_list = QA.QA_fetch_index_list_adv()

    if code is None:
        code_list = QA_fetch_index_info(list(code_list.code))
        codes = list(code_list[code_list.cate != '5'].code)
    else:
        codes = list(code_list[code_list.code.isin(code)].code)
    codes = codes + ['000001','399001','399006']
    codes = [i for i in codes if i not in ['880602','880604', '880650', '880608']]
    if type == 'crawl':
        res = QA_fetch_index_quant_pre_adv(codes,start_date,end_date, method=method,norm_type=norm_type).data
    if type == 'model':
        res = QA_fetch_get_index_quant_data(codes, start_date, end_date, type='normalization').set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_index_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    return(res)

def get_quant_data(start_date, end_date, code=None, type = 'crawl', block = False, sub_block= True, method = 'value', norm_type = 'normalization'):

    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, norm_type =norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_hedge_data(start_date, end_date, code=None, type = 'crawl', block = True, sub_block= True, method = 'value', norm_type = 'normalization'):
    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['沪深300'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, norm_type =norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_hedge_data_realtime(start_date, end_date, code=None, type = 'model', block = True, sub_block= True, method = 'value', norm_type = 'normalization'):
    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['沪深300'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_train_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data_realtime(codes, start_date, end_date, norm_type =norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_500hedge_data(start_date, end_date, code=None, type = 'crawl', block = True, sub_block= True, method = 'value', norm_type = 'normalization'):
    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['中证500'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, norm_type =norm_type).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, method=method)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def get_quant_data_hour(start_date, end_date, code=None, type = 'model', block = False, sub_block= True, method = 'value', norm_type = 'normalization'):

    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_hour_pre(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type)
    if type == 'model':
        res = QA_fetch_stock_quant_hour(codes, start_date, end_date, norm_type =norm_type).drop(['date'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, type='60min', method=method)
        res = res.join(target)
    return(res)

def get_index_quant_hour(start_date, end_date, code=None, type = 'crawl', method = 'value',norm_type=None):

    code_list = QA.QA_fetch_index_list_adv()

    if code is None:
        code_list = QA_fetch_index_info(list(code_list.code))
        codes = list(code_list[code_list.cate != '5'].code)
    else:
        codes = list(code_list[code_list.code.isin(code)].code)
    codes = codes + ['000001','399001','399006']

    codes = [i for i in codes if i not in ['880602','880604', '880650', '880608']]

    if type == 'crawl':
        res = QA_fetch_index_hour_pre(codes,start_date,end_date, method=method,norm_type=norm_type)
    if type == 'model':
        res = QA_fetch_index_quant_hour(codes, start_date, end_date, norm_type =norm_type).drop(['date'], axis=1)
        target = QA_fetch_index_target(codes, start_date, end_date, type='60min', method=method)
        res = res.join(target)
    return(res)

def get_quant_data_15min(start_date, end_date, code=None, type = 'model', block = False, sub_block= True, method = 'value', norm_type = 'normalization'):

    code_list = QA_fetch_stock_all()
    if code is None:
        codes = code_list
    else:
        codes = code_list[code_list.code.isin(code)]

    if block is True:
        data = QA.QA_fetch_stock_block()
        block = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
        codes = codes[codes.code.isin(block)]
    else:
        pass
    QA_util_log_info('##JOB Now Delete ST Stock')
    codes = codes[codes.name.apply(lambda x:x.count('ST')) == 0]
    codes = codes[codes.name.apply(lambda x:x.count('退')) == 0]
    codes = list(codes['code'])

    QA_util_log_info('##JOB Now Delete Stock Start With [688, 787, 789]')
    codes = [i for i in codes if i.startswith('688') == False]
    codes = [i for i in codes if i.startswith('787') == False]
    codes = [i for i in codes if i.startswith('789') == False]
    if type == 'crawl':
        res = QA_fetch_stock_hour_pre(codes,start_date,end_date, block = sub_block, method=method, norm_type =norm_type)
    if type == 'model':
        res = QA_fetch_stock_quant_hour(codes, start_date, end_date, norm_type =norm_type).drop(['date'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date, type='60min', method=method)
        res = res.join(target)
    return(res)

def get_index_quant_15min(start_date, end_date, code=None, type = 'crawl', method = 'value',norm_type=None):

    code_list = QA.QA_fetch_index_list_adv()

    if code is None:
        code_list = QA_fetch_index_info(list(code_list.code))
        codes = list(code_list[code_list.cate != '5'].code)
    else:
        codes = list(code_list[code_list.code.isin(code)].code)
    codes = codes + ['000001','399001','399006']

    codes = [i for i in codes if i not in ['880602','880604', '880650', '880608']]

    if type == 'crawl':
        res = QA_fetch_index_hour_pre(codes,start_date,end_date, method=method,norm_type=norm_type)
    if type == 'model':
        res = QA_fetch_index_quant_hour(codes, start_date, end_date, norm_type =norm_type).drop(['date'], axis=1)
        target = QA_fetch_index_target(codes, start_date, end_date, type='60min', method=method)
        res = res.join(target)
    return(res)

if __name__ == 'main':
    pass