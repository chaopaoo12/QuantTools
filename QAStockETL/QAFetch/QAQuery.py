
import numpy
import pandas as pd

from QUANTAXIS.QAUtil import (DATABASE, QA_Setting, QA_util_date_stamp,
                              QA_util_date_valid, QA_util_dict_remove_key,
                              QA_util_log_info, QA_util_code_tolist, QA_util_date_str2int, QA_util_date_int2str,
                              QA_util_sql_mongo_sort_DESCENDING,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse)
from QUANTAXIS.QAUtil.QADate import QA_util_pands_timestamp_to_date
from QAStockETL.FuncTools.financial_mean import financial_dict

def QA_fetch_financial_report(code, report_date, type ='report', ltype='EN', db=DATABASE):
    """获取专业财务报表

    Arguments:
        code {[type]} -- [description]
        report_date {[type]} -- [description]

    Keyword Arguments:
        ltype {str} -- [description] (default: {'EN'})
        db {[type]} -- [description] (default: {DATABASE})

    Raises:
        e -- [description]

    Returns:
        pd.DataFrame -- [description]
    """

    if isinstance(code, str):
        code = [code]
    if isinstance(report_date, str):
        report_date = [QA_util_date_str2int(report_date)]
    elif isinstance(report_date, int):
        report_date = [report_date]
    elif isinstance(report_date, list):
        report_date = [QA_util_date_str2int(item) for item in report_date]

    collection = db.financial
    num_columns = [item[:3] for item in list(financial_dict.keys())]
    CH_columns = [item[3:] for item in list(financial_dict.keys())]
    EN_columns = list(financial_dict.values())
    #num_columns.extend(['283', '_id', 'code', 'report_date'])
    # CH_columns.extend(['283', '_id', 'code', 'report_date'])
    #CH_columns = pd.Index(CH_columns)
    #EN_columns = list(financial_dict.values())
    #EN_columns.extend(['283', '_id', 'code', 'report_date'])
    #EN_columns = pd.Index(EN_columns)


    try:
        if type == 'report':
            if code is not None and report_date is not None:
                data = [item for item in collection.find(
                    {'code': {'$in': code}, 'report_date': {'$in': report_date}}, batch_size=10000)]
            elif code is None and report_date is not None:
                data = [item for item in collection.find(
                    {'report_date': {'$in': report_date}}, batch_size=10000)]
            elif code is not None and report_date is None:
                data = [item for item in collection.find(
                    {'code': {'$in': code}}, batch_size=10000)]
            else:
                data = [item for item in collection.find()]

        elif type == 'date':
            if code is not None and report_date is not None:
                data = [item for item in collection.find(
                    {'code': {'$in': code}, 'crawl_date': {'$in': report_date}}, batch_size=10000)]
            elif code is None and report_date is not None:
                data = [item for item in collection.find(
                    {'crawl_date': {'$in': report_date}}, batch_size=10000)]
            elif code is not None and report_date is None:
                data = [item for item in collection.find(
                    {'code': {'$in': code}}, batch_size=10000)]
            else:
                data = [item for item in collection.find()]
        else:
            print("type must be date or report")

        if len(data) > 0:
            res_pd = pd.DataFrame(data)
            if ltype in ['CH', 'CN']:

                cndict = dict(zip(num_columns, CH_columns))
                cndict['283']='283'
                try:
                    cndict['284'] = '284'
                    cndict['285'] = '285'
                    cndict['286'] = '286'
                except:
                    pass
                cndict['_id']='_id'
                cndict['code']='code'
                cndict['report_date']='report_date'
                cndict['crawl_date']='crawl_date'
                res_pd.columns = res_pd.columns.map(lambda x: cndict[x])
            elif ltype is 'EN':
                endict=dict(zip(num_columns,EN_columns))
                endict['283']='283'
                try:
                    endict['284'] = '284'
                    endict['285'] = '285'
                    endict['286'] = '286'
                except:
                    pass
                endict['_id']='_id'
                endict['code']='code'
                endict['report_date']='report_date'
                endict['crawl_date']='crawl_date'
                res_pd.columns = res_pd.columns.map(lambda x: endict[x])

            if res_pd.report_date.dtype == numpy.int64:
                res_pd.report_date = pd.to_datetime(
                    res_pd.report_date.apply(QA_util_date_int2str))
            else:
                res_pd.report_date = pd.to_datetime(res_pd.report_date)

            return res_pd.replace(-4.039810335e+34, numpy.nan).set_index(['report_date', 'code'], drop=False)
        else:
            return None
    except Exception as e:
        raise e

def QA_fetch_stock_financial_calendar(code, start, end=None, format='pd', collections=DATABASE.report_calendar):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "real_date": {
                "$lte": end,
                "$gte": start}}, {"_id": 0}, batch_size=10000)
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            res = res.drop_duplicates(
                (['report_date', 'code']))
            res = res.ix[:, ['code', 'name', 'pre_date', 'first_date', 'second_date',
                             'third_date', 'real_date', 'codes', 'report_date', 'crawl_date']]
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_stock_financial_calendar format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_financial_calendar data parameter start=%s end=%s is not right' % (start, end))


def QA_fetch_stock_divyield(code, start, end=None, format='pd', collections=DATABASE.stock_divyield):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        __data = []
        cursor = collections.find({
            'a_stockcode': {'$in': code}, "dir_dcl_date": {
                "$lte": end,
                "$gte": start}}, {"_id": 0}, batch_size=10000)
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            res = res.drop_duplicates(
                (['dir_dcl_date', 'a_stockcode']))
            res = res.ix[:, ['a_stockcode', 'a_stocksname', 'div_info', 'div_type_code', 'bonus_shr',
                             'cash_bt', 'cap_shr', 'epsp', 'ps_cr', 'ps_up', 'reg_date', 'dir_dcl_date',
                             'a_stockcode1', 'ex_divi_date', 'prg']]
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_stock_divyield format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_divyield data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_financial_TTM(code, start, end = None, format='pd', collections=DATABASE.financial_TTM):
    '获取财报TTM数据'
    #code= [code] if isinstance(code,str) else code
    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        __data = []

        cursor = collections.find({
            'CODE': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}}, batch_size=10000)
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            res = res.drop('_id', axis=1).drop_duplicates((['REPORT_DATE', 'CODE']))
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_financial_TTM format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_financial_TTM data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_stock_fianacial(code, start, end = None, format='pd', collections=DATABASE.stock_financial_analysis):
    '获取财报TTM数据'
    #code= [code] if isinstance(code,str) else code
    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        cursor = collections.find({
            'CODE': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}}, batch_size=10000)
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]
        res = pd.DataFrame([item for item in cursor])

        try:
            res =  res.drop('_id', axis=1).assign(DATE=pd.to_datetime(
                res.DATE, unit='ms')).drop_duplicates((['CODE', 'DATE']))
            #res.columns = [i.lower() for i in list(res.columns)]

        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            #res['report_date'] = pd.to_datetime(res['report_date']/1000, unit='s')
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_financial_TTM format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_financial_TTM data parameter start=%s end=%s is not right' % (start, end))
