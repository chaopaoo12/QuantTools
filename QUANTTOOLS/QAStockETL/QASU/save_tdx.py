from QUANTAXIS import QA_fetch_get_usstock_list,QA_fetch_get_index_list,QA_fetch_get_index_day
import datetime
from QUANTAXIS.QAUtil import (QA_Setting, QA_util_date_stamp,
                              QA_util_date_str2int, QA_util_date_valid,
                              QA_util_get_real_date, QA_util_get_real_datelist,
                              QA_util_future_to_realdatetime, QA_util_tdxtimestamp,
                              QA_util_future_to_tradedatetime,
                              QA_util_get_trade_gap, QA_util_log_info,
                              QA_util_time_stamp, QA_util_web_ping,
                              exclude_from_stock_ip_list, future_ip_list,
                              stock_ip_list, trade_date_sse)
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import pandas as pd
import pymongo
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_get_next_day,
    QA_util_get_real_date,
    QA_util_log_info,
    QA_util_to_json_from_pandas,
    trade_date_sse
)
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import (QA_fetch_get_usstock_adj,QA_fetch_get_usstock_day,QA_fetch_get_usstock_cik,
                                                 QA_fetch_get_usstock_financial, QA_fetch_get_usstock_financial_calendar,
                                                 QA_fetch_get_stock_industryinfo,QA_fetch_get_index_info,
                                                 QA_fetch_get_stock_delist)


def now_time():
    return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
           ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
        str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def QA_SU_save_usstock_list(client=DATABASE, ui_log=None, ui_progress=None):
    """save usstock_list

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    try:
        QA_util_log_info(
            '##JOB16 Now Saving USSTOCK_LIST ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        usstock_list_from_tdx = QA_fetch_get_usstock_list()
        pandas_data = QA_util_to_json_from_pandas(usstock_list_from_tdx)

        if len(pandas_data) > 0:
            # 获取到数据后才进行drop collection 操作
            client.drop_collection('usstock_list')
            coll = client.usstock_list
            coll.create_index('code')
            coll.insert_many(pandas_data)
        QA_util_log_info(
            "完成USSTOCK列表获取",
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=10000
        )
    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        QA_util_log_info(" Error save_tdx.QA_SU_save_usstock_list exception!")
        pass

def QA_SU_save_usstock_day(client=DATABASE, ui_log=None, ui_progress=None):
    """save usstock_day

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    __usstock_list = QA_fetch_get_usstock_list().code.unique().tolist()
    coll = client.usstock_day
    coll.create_index(
        [('code',
          pymongo.ASCENDING),
         ('date_stamp',
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll):

        try:

            ref_ = coll.find({'code': str(code)[0:6]})
            end_time = str(now_time())[0:10]
            if ref_.count() > 0:
                start_time = ref_[ref_.count() - 1]['date']

                QA_util_log_info(
                    '##JOB06 Now Saving USSTOCK_DAY==== \n Trying updating {} from {} to {}'
                        .format(code,
                                start_time,
                                end_time),
                    ui_log=ui_log
                )

                if start_time != end_time:
                    coll.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_usstock_day(
                                str(code),
                                QA_util_get_next_day(start_time),
                                end_time
                            )
                        )
                    )
            else:
                start_time = '1990-01-01'
                QA_util_log_info(
                    '##JOB06 Now Saving USSTOCK_DAY==== \n Trying updating {} from {} to {}'
                        .format(code,
                                start_time,
                                end_time),
                    ui_log=ui_log
                )

                if start_time != end_time:
                    coll.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_usstock_day(
                                str(code),
                                start_time,
                                end_time
                            )
                        )
                    )
        except:
            err.append(str(code))

    for i_ in range(len(__usstock_list)):
        # __saving_work('000001')
        QA_util_log_info(
            'The {} of Total {}'.format(i_,
                                        len(__usstock_list)),
            ui_log=ui_log
        )

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(__usstock_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(i_ / len(__usstock_list) * 10000.0))
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )

        __saving_work(__usstock_list[i_], coll)

    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_usstock_adj(client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    stock_list = QA_fetch_get_usstock_list().code.unique().tolist()
    # client.drop_collection('stock_xdxr')

    try:
        coll_adj = client.usstock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
    except:
        client.drop_collection('usstock_adj')
        coll_adj = client.stock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )

    err = []

    def __saving_work(code):
        QA_util_log_info(
            '##JOB02 Now Saving XDXR INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            qfq = QA_fetch_get_usstock_adj(stock_list)
            qfq = qfq.assign(date=qfq.date.apply(lambda x: str(x)[0:10]))
            adjdata = QA_util_to_json_from_pandas(qfq.loc[:, ['date','code', 'adj']])
            coll_adj.delete_many({'code': code})
            #print(adjdata)
            coll_adj.insert_many(adjdata)


        except Exception as e:
            print(e)

        err.append(str(code))

    for i_ in range(len(stock_list)):
        QA_util_log_info(
            'The {} of Total {}'.format(i_,
                                        len(stock_list)),
            ui_log=ui_log
        )
        strLogInfo = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(stock_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(i_ / len(stock_list) * 100))
        QA_util_log_info(
            strLogInfo,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )
        __saving_work(stock_list[i_])

def QA_SU_save_usstock_cik(client=DATABASE, ui_log=None, ui_progress=None):
    """save usstock_cik

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    try:
        QA_util_log_info(
            '##JOB16 Now Saving USSTOCK_CIK ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        usstock_cik_from_tdx = QA_fetch_get_usstock_cik()
        pandas_data = QA_util_to_json_from_pandas(usstock_cik_from_tdx)

        if len(pandas_data) > 0:
            # 获取到数据后才进行drop collection 操作
            client.drop_collection('usstock_cik')
            coll = client.usstock_cik
            coll.create_index('code')
            coll.insert_many(pandas_data)
        QA_util_log_info(
            "完成USSTOCK列表获取",
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=10000
        )
    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        QA_util_log_info(" Error save_tdx.QA_SU_save_usstock_cik exception!")
        pass

def QA_SU_save_usstock_financial_files(client=DATABASE, ui_log=None, ui_progress=None):
    pass

def QA_SU_save_usstock_report_calendar_day(client=DATABASE, ui_log=None, ui_progress=None):
    pass

def QA_SU_save_stock_industryinfo(client=DATABASE, ui_log=None, ui_progress=None):

    client.drop_collection('stock_industryinfo')
    coll = client.stock_industryinfo
    coll.create_index('code')
    err = []

    """save stock_block

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    client.drop_collection('stock_industryinfo')
    coll = client.stock_industryinfo
    coll.create_index('code')

    try:
        QA_util_log_info(
            '##JOB09 Now Saving STOCK_INDUSTRY_INFO ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        coll.insert_many(
            QA_util_to_json_from_pandas(QA_fetch_get_stock_industryinfo())
        )

    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.QA_SU_save_stock_industryinfo exception!")
        pass


def QA_SU_save_index_info(client=DATABASE, ui_log=None, ui_progress=None):
    client.drop_collection('index_info')
    coll = client.index_info
    coll.create_index('code')
    err = []

    try:
        QA_util_log_info(
            '##JOB09 Now Saving INDEX_INFO ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        coll.insert_many(
            QA_util_to_json_from_pandas(QA_fetch_get_index_info())
        )

    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.QA_SU_save_index_info exception!")
        pass


def QA_SU_save_stock_delist(client=DATABASE, ui_log=None, ui_progress=None):
    client.drop_collection('stock_delist')
    coll = client.stock_delist
    coll.create_index('code')
    err = []

    try:
        QA_util_log_info(
            '##JOB09 Now Saving STOCK_DELIST ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        coll.insert_many(
            QA_util_to_json_from_pandas(QA_fetch_get_stock_delist())
        )

    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.QA_SU_save_stock_delist exception!")
        pass

def QA_SU_save_index_week(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_week

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    stock_list = QA_fetch_get_index_list('tdx').code.unique().tolist()
    coll_index_week = client.index_week
    coll_index_week.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_index_week):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving INDEX_WEEK==== {}'.format(str(code)),
                ui_log=ui_log
            )

            ref = coll_index_week.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]
            if ref.count() > 0:
                # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现

                start_date = ref[ref.count() - 1]['date']

                QA_util_log_info(
                    'UPDATE_INDEX_WEEK \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_week.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                QA_util_get_next_day(start_date),
                                end_date,
                                level='week'
                            )
                        )
                    )
            else:
                start_date = '1990-01-01'
                QA_util_log_info(
                    'UPDATE_STOCK_WEEK \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_week.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                start_date,
                                end_date,
                                level='week'
                            )
                        )
                    )
        except:
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info(
            'The {} of Total {}'.format(item,
                                        len(stock_list)),
            ui_log=ui_log
        )
        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%'
        )
        intProgress = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )

        __saving_work(stock_list[item], coll_index_week)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_index_month(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_week

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    stock_list = QA_fetch_get_index_list('tdx').code.unique().tolist()
    coll_index_month = client.index_month
    coll_index_month.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_index_month):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving INDEX_MONTH==== {}'.format(str(code)),
                ui_log=ui_log
            )

            ref = coll_index_month.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]
            if ref.count() > 0:
                # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现

                start_date = ref[ref.count() - 1]['date']

                QA_util_log_info(
                    'UPDATE_INDEX_MONTH \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_month.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                QA_util_get_next_day(start_date),
                                end_date,
                                level='month'
                            )
                        )
                    )
            else:
                start_date = '1990-01-01'
                QA_util_log_info(
                    'UPDATE_INDEX_MONTH \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_month.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                start_date,
                                end_date,
                                level='month'
                            )
                        )
                    )
        except:
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info(
            'The {} of Total {}'.format(item,
                                        len(stock_list)),
            ui_log=ui_log
        )
        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%'
        )
        intProgress = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )

        __saving_work(stock_list[item], coll_index_month)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_index_year(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_week

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    stock_list = QA_fetch_get_index_list('tdx').code.unique().tolist()
    coll_index_year = client.index_year
    coll_index_year.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_index_year):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving INDEX_YEAR==== {}'.format(str(code)),
                ui_log=ui_log
            )

            ref = coll_index_year.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]
            if ref.count() > 0:
                # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现

                start_date = ref[ref.count() - 1]['date']

                QA_util_log_info(
                    'UPDATE_INDEX_YEAR \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_year.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                QA_util_get_next_day(start_date),
                                end_date,
                                level='year'
                            )
                        )
                    )
            else:
                start_date = '1990-01-01'
                QA_util_log_info(
                    'UPDATE_INDEX_YEAR \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log=ui_log
                )
                if start_date != end_date:
                    coll_index_year.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_index_day(
                                str(code),
                                start_date,
                                end_date,
                                level='year'
                            )
                        )
                    )
        except:
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info(
            'The {} of Total {}'.format(item,
                                        len(stock_list)),
            ui_log=ui_log
        )
        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%'
        )
        intProgress = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )

        __saving_work(stock_list[item], coll_index_year)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)