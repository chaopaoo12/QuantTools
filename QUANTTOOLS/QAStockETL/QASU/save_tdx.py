from QUANTAXIS import (QA_fetch_get_usstock_list,QA_fetch_get_index_list,QA_fetch_get_index_day,
                       QA_fetch_get_stock_day,QA_fetch_get_stock_xdxr,QA_fetch_get_stock_info,
                       )
from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_list, QA_fetch_get_stock_min, QA_fetch_get_index_min

from QUANTTOOLS.QAStockETL.QAFetch.QATdx import (QA_fetch_get_usstock_adj,QA_fetch_get_usstock_day,QA_fetch_get_usstock_cik,
                                                 QA_fetch_get_usstock_financial, QA_fetch_get_usstock_financial_calendar,
                                                 QA_fetch_get_stock_industryinfo,QA_fetch_get_index_info,
                                                 QA_fetch_get_stock_industry,
                                                 QA_fetch_get_stock_delist,QA_fetch_get_stock_half,

                                                 QA_fetch_get_usstock_pe,QA_fetch_get_usstock_pb)
from QUANTTOOLS.QAStockETL.QAFetch import (fetch_get_stock_code_all,QA_fetch_get_stock_etlreal,
                                           QA_fetch_get_usstock_list_sina, QA_fetch_get_usstock_list_akshare,
                                           QA_fetch_get_stock_half_realtime, QA_fetch_get_usstock_day_xq,

                                           QA_fetch_stock_om_all,QA_fetch_stock_all,QA_fetch_usstock_day,)
from QUANTAXIS.QAData.data_fq import _QA_data_stock_to_fq
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_day
import concurrent
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import datetime
import pymongo
import pandas as pd
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_get_next_day,
    QA_util_get_real_date,
    QA_util_log_info,
    QA_util_to_json_from_pandas,
    QA_util_today_str,
    trade_date_sse,
    QA_util_date_stamp,
    QA_util_code_tolist
)



def now_time():
    return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
           ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
        str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def QA_SU_save_stock_half(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    stock_list = QA_fetch_stock_all().code.unique().tolist()
    coll_stock_day = client.stock_day_half
    coll_stock_day.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_stock_day):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving STOCK_DAY_HALF==== {}'.format(str(code)),
                ui_log
            )

            # 首选查找数据库 是否 有 这个代码的数据
            ref = coll_stock_day.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref.count() > 0:

                # 接着上次获取的日期继续更新
                start_date = str(ref[ref.count() - 1]['date'])[0:10]
                QA_util_log_info(
                    'UPDATE_STOCK_DAY_HALF \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_stock_half(str(code),
                                                   QA_util_get_next_day(start_date),
                                                   end_date
                                                   )
                        )
                    )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '2010-01-01'
                QA_util_log_info(
                    'UPDATE_STOCK_DAY_HALF \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_stock_half(
                                                   str(code),
                                                   start_date,
                                                   end_date
                                                   )
                        )
                    )
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info('The {} of Total {}'.format(item, len(stock_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%',
            ui_log
        )
        intProgressToLog = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgressToLog,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgressToLog
        )

        __saving_work(stock_list[item], coll_stock_day)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock day half ^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_day(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    stock_list = QA_fetch_stock_all().code.unique().tolist()
    coll_stock_day = client.stock_day
    coll_stock_day.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_stock_day):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving STOCK_DAY==== {}'.format(str(code)),
                ui_log
            )

            # 首选查找数据库 是否 有 这个代码的数据
            ref = coll_stock_day.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref.count() > 0:

                # 接着上次获取的日期继续更新
                start_date = ref[ref.count() - 1]['date']

                QA_util_log_info(
                    'UPDATE_STOCK_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_stock_day('tdx',
                                str(code),
                                QA_util_get_next_day(start_date),
                                end_date,
                                '00'
                            )
                        )
                    )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '1990-01-01'
                QA_util_log_info(
                    'UPDATE_STOCK_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_stock_day('tdx',
                                str(code),
                                start_date,
                                end_date,
                                '00'
                            )
                        )
                    )
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info('The {} of Total {}'.format(item, len(stock_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%',
            ui_log
        )
        intProgressToLog = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgressToLog,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgressToLog
        )

        __saving_work(stock_list[item], coll_stock_day)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock day ^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)

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
        usstock_list_from_tdx = QA_fetch_get_usstock_list_akshare()
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

    __usstock_list = QA_fetch_get_usstock_list_akshare().code.unique().tolist()

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
    stock_list = QA_fetch_get_usstock_list_akshare().code.unique().tolist()
    # client.drop_collection('stock_xdxr')

    try:
        coll_adj = client.usstock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )
    except:
        client.drop_collection('usstock_adj')
        coll_adj = client.usstock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )

    err = []

    def __saving_work(code):
        QA_util_log_info(
            '##JOB02 Now Saving US ADJ INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            qfq = QA_fetch_get_usstock_adj(code)
            market_day = QA_fetch_usstock_day(str(code), '1990-01-01', QA_util_today_str(), 'pd')
            data2 = pd.concat([market_day, qfq.set_index('date')[['adj','adjust']]],axis=1)
            data2[['adj','adjust']] = data2[['adj','adjust']].fillna(method='ffill')
            data2 = data2[~data2.code.isna()].reset_index(drop=True)
            data2 = data2.assign(date_stamp=data2.date.apply(lambda x:QA_util_date_stamp(str(x).replace('1900','1971'))))
            adjdata = QA_util_to_json_from_pandas(data2[['date','code', 'adj', 'adjust', 'date_stamp']])
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

def QA_SU_save_usstock_pb(client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    stock_list = QA_fetch_get_usstock_list_akshare().code.unique().tolist()
    # client.drop_collection('stock_xdxr')

    try:
        coll = client.usstock_pb
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )
    except:
        client.drop_collection('usstock_pb')
        coll = client.coll
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )

    err = []

    def __saving_work(code):
        QA_util_log_info(
            '##JOB02 Now Saving USSTOCK PB INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            data = QA_fetch_get_usstock_pb(code)
            data = QA_util_to_json_from_pandas(data)
            coll.delete_many({'code': code})
            #print(adjdata)
            coll.insert_many(data)


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

def QA_SU_save_usstock_pe(client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    stock_list = QA_fetch_get_usstock_list_akshare().code.unique().tolist()
    # client.drop_collection('stock_xdxr')

    try:
        coll = client.usstock_pe
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )
    except:
        client.drop_collection('usstock_pe')
        coll = client.coll
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)],
            unique=True
        )

    err = []

    def __saving_work(code):
        QA_util_log_info(
            '##JOB02 Now Saving USSTOCK PE INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            data = QA_fetch_get_usstock_pe(code)
            data = QA_util_to_json_from_pandas(data)
            coll.delete_many({'code': code})
            #print(adjdata)
            coll.insert_many(data)


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

    stock_all = QA_fetch_get_stock_list()[['code','name']]
    try:
        QA_util_log_info(
            '##JOB09 Now Saving STOCK_INDUSTRY_INFO ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        coll.insert_many(
            QA_util_to_json_from_pandas(QA_fetch_get_stock_industry(stock_all))
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

def QA_SU_save_stock_xdxr(client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    stock_list = QA_fetch_stock_all().code.unique().tolist()
    # client.drop_collection('stock_xdxr')

    try:
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        coll_adj = client.stock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        QA_util_log_info(
            '##JOB01 Now XDXR INFO Create ==== ',
            ui_log=ui_log
        )
    except:
        client.drop_collection('stock_xdxr')
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        client.drop_collection('stock_adj')
        coll_adj = client.stock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        QA_util_log_info(
            '##JOB01 Now XDXR INFO Rebuild ==== ',
            ui_log=ui_log
        )

    err = []

    def __saving_work(code, coll):
        QA_util_log_info(
            '##JOB02 Now Saving XDXR INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            xdxr  = QA_fetch_get_stock_xdxr('tdx',str(code))
            try:
                coll.insert_many(
                    QA_util_to_json_from_pandas(xdxr),
                    ordered=False
                )
            except:
                pass
            try:
                data = QA_fetch_stock_day(str(code), '1990-01-01',str(datetime.date.today()), 'pd')
                qfq = _QA_data_stock_to_fq(data, xdxr, 'qfq')
                qfq = qfq.assign(date=qfq.date.apply(lambda x: str(x)[0:10]))
                adjdata = QA_util_to_json_from_pandas(qfq.loc[:, ['date','code', 'adj']])
                coll_adj.delete_many({'code': code})
                #print(adjdata)
                coll_adj.insert_many(adjdata)
                QA_util_log_info(
                    '##JOB03 Now Saving XDXR INFO SUCCESS ==== {}'.format(str(code)),
                    ui_log=ui_log
                )

            except Exception as e:
                print(e)


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
        __saving_work(stock_list[i_], coll)

def QA_SU_save_stock_info(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    client.drop_collection('stock_info')
    stock_list = QA_fetch_stock_all().code.unique().tolist()
    coll = client.stock_info
    coll.create_index('code')
    err = []

    def __saving_work(code, coll):
        QA_util_log_info(
            '##JOB10 Now Saving STOCK INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            coll.insert_many(
                QA_util_to_json_from_pandas(QA_fetch_get_stock_info('tdx',str(code)))
            )

        except:
            err.append(str(code))

    for i_ in range(len(stock_list)):
        # __saving_work('000001')

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(stock_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(i_ / len(stock_list) * 10000.0))
        QA_util_log_info('The {} of Total {}'.format(i_, len(stock_list)))
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )

        __saving_work(stock_list[i_], coll)
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_stock_real(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    #client.drop_collection('stock_real')
    stock_list = QA_fetch_stock_all().code.unique().tolist()
    coll = client.stock_real
    coll.create_index([('code',
                        pymongo.ASCENDING),
                       ('date_stamp',
                        pymongo.ASCENDING)],
                      unique=True)
    err = []

    def __saving_work(stock_list, coll):
        QA_util_log_info(
            '##JOB10 Now Saving STOCK REAL ==== ',
            ui_log=ui_log
        )
        data = QA_fetch_get_stock_half_realtime(stock_list)
        try:
            coll.insert_many(
                QA_util_to_json_from_pandas(data)
            )

        except Exception as error0:
            print(error0)
            err.append(error0)
    __saving_work(stock_list, coll)

    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_usstock_xq_day(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    stock_list = QA_fetch_get_usstock_list_akshare().code.unique().tolist()
    coll_stock_day = client.usstock_day_xq
    coll_stock_day.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )
    err = []

    def __saving_work(code, coll_stock_day):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving US_STOCK_XQ_DAY==== {}'.format(str(code)),
                ui_log
            )

            # 首选查找数据库 是否 有 这个代码的数据
            ref = coll_stock_day.find({'code': str(code)[0:6]})
            end_date = str(now_time())[0:10]

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref.count() > 0:

                # 接着上次获取的日期继续更新
                start_date = str(ref[ref.count() - 1]['date'])[0:10]
                QA_util_log_info(
                    'UPDATE_US_STOCK_XQ_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_usstock_day_xq(str(code),
                                                    QA_util_get_next_day(start_date),
                                                    end_date
                                                    )
                        )
                    )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '2016-01-01'
                QA_util_log_info(
                    'UPDATE_US_STOCK_XQ_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_usstock_day_xq(
                                str(code),
                                start_date,
                                end_date
                            )
                        )
                    )
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in range(len(stock_list)):
        QA_util_log_info('The {} of Total {}'.format(item, len(stock_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%',
            ui_log
        )
        intProgressToLog = int(float(item / len(stock_list) * 100))
        QA_util_log_info(
            strProgressToLog,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgressToLog
        )

        __saving_work(stock_list[item], coll_stock_day)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save us stock xq day^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_hour(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_stock_min(['60min'])

def QA_SU_save_stock_30min(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_stock_min(['30min'])

def QA_SU_save_stock_15min(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_stock_min(['15min'])

def QA_SU_save_single_stock_hour(code : str, client=DATABASE, ui_log=None, ui_progress=None):
    """save single stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_single_stock_min(code, ['60min'])

def QA_SU_save_single_stock_30min(code : str, client=DATABASE, ui_log=None, ui_progress=None):
    """save single stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_single_stock_min(code, ['30min'])

def QA_SU_save_single_stock_15min(code : str, client=DATABASE, ui_log=None, ui_progress=None):
    """save single stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_single_stock_min(code, ['15min'])

def QA_SU_save_single_stock_xdxr(code : str, client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    stock_list = [code]
    # client.drop_collection('stock_xdxr')

    try:
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        coll_adj = client.stock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        QA_util_log_info(
            '##JOB01 Now Saving XDXR INFO Create ====',
            ui_log=ui_log
        )
    except:
        #client.drop_collection('stock_xdxr')
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        #client.drop_collection('stock_adj')
        coll_adj = client.stock_adj
        coll_adj.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
        QA_util_log_info(
            '##JOB01 Now Saving XDXR INFO Rebuild ====',
            ui_log=ui_log
        )

    err = []

    def __saving_work(code, coll):
        QA_util_log_info(
            '##JOB02 Now Saving XDXR INFO ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:

            xdxr  = QA_fetch_get_stock_xdxr('tdx',str(code))
            try:
                coll.insert_many(
                    QA_util_to_json_from_pandas(xdxr),
                    ordered=False
                )
            except:
                pass
            try:
                data = QA_fetch_stock_day(str(code), '1990-01-01',str(datetime.date.today()), 'pd')
                qfq = _QA_data_stock_to_fq(data, xdxr, 'qfq')
                qfq = qfq.assign(date=qfq.date.apply(lambda x: str(x)[0:10]))
                adjdata = QA_util_to_json_from_pandas(qfq.loc[:, ['date','code', 'adj']])
                coll_adj.delete_many({'code': code})
                #print(adjdata)
                coll_adj.insert_many(adjdata)
                QA_util_log_info(
                    '##JOB03 Now Saving XDXR INFO SUCCESS ==== {}'.format(str(code)),
                    ui_log=ui_log
                )

            except Exception as e:
                print(e)


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
        __saving_work(stock_list[i_], coll)

def QA_SU_save_stock_aklist(client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_list

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    client.drop_collection('akstock_list')
    coll = client.akstock_list
    coll.create_index('code')

    try:
        # 🛠todo 这个应该是第一个任务 JOB01， 先更新股票列表！！
        QA_util_log_info(
            '##JOB08 Now Saving STOCK_LIST from AKShare ====',
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        stock_list_from_tdx = fetch_get_stock_code_all()
        pandas_data = QA_util_to_json_from_pandas(stock_list_from_tdx)
        coll.insert_many(pandas_data)
        QA_util_log_info(
            "完成股票列表获取",
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=10000
        )
    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.QA_SU_save_stock_aklist exception!")

        pass

def QA_SU_save_stock_basereal(code =None, start_date = None, end_date = None, client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    if code is None:
        stock_list = QA_fetch_stock_all().code.unique().tolist()
    else:
        stock_list = QA_util_code_tolist(code)
    stock_basereal = client.stock_basereal
    stock_basereal.create_index([('code',
                        pymongo.ASCENDING),
                       ('date_stamp',
                        pymongo.ASCENDING)],
                      unique=True)
    err = []

    def __saving_work(code, start_date, end_date):
        QA_util_log_info(
            '##JOB10 Now Saving STOCK BASE REAL ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        data = QA_fetch_get_stock_etlreal(code, start_date, end_date)
        try:
            stock_basereal.insert_many(
                QA_util_to_json_from_pandas(data)
            )

        except Exception as error0:
            print(error0)
            err.append(error0)

    for code in stock_list:
        QA_util_log_info('The {} of Total {}'.format
                         ((stock_list.index(code) +1), len(stock_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((stock_list.index(code) +1) / len(stock_list) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((stock_list.index(code) +1) / len(stock_list) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work(code, start_date, end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_index_hour(client=DATABASE, ui_log=None, ui_progress=None):
    """save index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_index_min(['60min'])

def QA_SU_save_index_15min(client=DATABASE, ui_log=None, ui_progress=None):
    """save index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_index_min(['15min'])

def QA_SU_save_single_index_hour(code : str):
    """save single index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_single_index_min(code, ['60min'])

def QA_SU_save_single_index_15min(code : str):
    """save single index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    QA_SU_save_single_index_min(code, ['15min'])

def QA_SU_save_stock_min(time_type : list,client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    stock_list = QA_fetch_get_stock_list().code.unique().tolist()
    coll = client.stock_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ]
    )
    err = []

    def __saving_work(code, coll, time_type):
        QA_util_log_info(
            '##JOB03 Now Saving STOCK_MIN ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            for type in time_type:
                ref_ = coll.find({'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    QA_util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )
                    if start_time != end_time:
                        __data = QA_fetch_get_stock_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)[1::]
                            )
                else:
                    start_time = '2015-01-01'
                    QA_util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )
                    if start_time != end_time:
                        __data = QA_fetch_get_stock_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)
                            )
        except Exception as e:
            QA_util_log_info(e, ui_log=ui_log)
            err.append(code)
            QA_util_log_info(err, ui_log=ui_log)

    executor = ThreadPoolExecutor(max_workers=4)
    # executor.map((__saving_work,  stock_list[i_], coll),URLS)
    res = {
        executor.submit(__saving_work,
                        stock_list[i_],
                        coll,
                        time_type)
        for i_ in range(len(stock_list))
    }
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(stock_list)),
            ui_log=ui_log
        )

        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(stock_list) * 100))[0:4] + '%'
        )
        intProgress = int(count / len(stock_list) * 10000.0)
        QA_util_log_info(
            strProgress,
            ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_single_stock_min(code : str, time_type : list, client=DATABASE, ui_log=None, ui_progress=None):
    """save single stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    #stock_list = QA_fetch_get_stock_list().code.unique().tolist()
    stock_list = [code]
    coll = client.stock_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ]
    )
    err = []

    def __saving_work(code, coll, time_type):
        QA_util_log_info(
            '##JOB03 Now Saving STOCK_MIN ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:
            for type in time_type:
                ref_ = coll.find({'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    QA_util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )
                    if start_time != end_time:
                        __data = QA_fetch_get_stock_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)[1::]
                            )
                else:
                    start_time = '2015-01-01'
                    QA_util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )
                    if start_time != end_time:
                        __data = QA_fetch_get_stock_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)
                            )
        except Exception as e:
            QA_util_log_info(e, ui_log=ui_log)
            err.append(code)
            QA_util_log_info(err, ui_log=ui_log)

    executor = ThreadPoolExecutor(max_workers=4)
    # executor.map((__saving_work,  stock_list[i_], coll),URLS)
    res = {
        executor.submit(__saving_work,
                        stock_list[i_],
                        coll,
                        time_type)
        for i_ in range(len(stock_list))
    }
    count = 1
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(stock_list)),
            ui_log=ui_log
        )

        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(stock_list) * 100))[0:4] + '%'
        )
        intProgress = int(count / len(stock_list) * 10000.0)
        QA_util_log_info(
            strProgress,
            ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)


def QA_SU_save_index_min(time_type : list,client=DATABASE, ui_log=None, ui_progress=None):
    """save index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    __index_list = QA_fetch_get_stock_list('index')
    coll = client.index_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ]
    )
    err = []

    def __saving_work(code, coll, time_type):

        QA_util_log_info(
            '##JOB05 Now Saving Index_MIN ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:

            for type in time_type:
                ref_ = coll.find({'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    QA_util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )

                    if start_time != end_time:
                        __data = QA_fetch_get_index_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data[1::])
                            )
                else:
                    start_time = '2015-01-01'

                    QA_util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )

                    if start_time != end_time:
                        __data = QA_fetch_get_index_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)
                            )
        except:
            err.append(code)

    executor = ThreadPoolExecutor(max_workers=4)

    res = {
        executor.submit(__saving_work,
                        __index_list.index[i_][0],
                        coll,
                        time_type)
        for i_ in range(len(__index_list))
    }  # multi index ./.
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(__index_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(count / len(__index_list) * 10000.0))
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(__index_list)),
            ui_log=ui_log
        )
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)

def QA_SU_save_single_index_min(code : str, time_type : list, client=DATABASE, ui_log=None, ui_progress=None):
    """save single index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    #__index_list = QA_fetch_get_stock_list('index')
    __index_list = [code]
    coll = client.index_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ]
    )
    err = []

    def __saving_work(code, coll, time_type):

        QA_util_log_info(
            '##JOB05 Now Saving Index_MIN ==== {}'.format(str(code)),
            ui_log=ui_log
        )
        try:

            for type in time_type:
                ref_ = coll.find({'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    QA_util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )

                    if start_time != end_time:
                        __data = QA_fetch_get_index_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data[1::])
                            )
                else:
                    start_time = '2015-01-01'

                    QA_util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.format(
                            ['1min',
                             '5min',
                             '15min',
                             '30min',
                             '60min'].index(type),
                            str(code),
                            start_time,
                            end_time,
                            type
                        ),
                        ui_log=ui_log
                    )

                    if start_time != end_time:
                        __data = QA_fetch_get_index_min(
                            str(code),
                            start_time,
                            end_time,
                            type
                        )
                        if len(__data) > 1:
                            coll.insert_many(
                                QA_util_to_json_from_pandas(__data)
                            )
        except:
            err.append(code)

    executor = ThreadPoolExecutor(max_workers=4)

    res = {
        executor.submit(__saving_work,
                        __index_list[i_],
                        coll,
                        time_type)
        for i_ in range(len(__index_list))
    }  # multi index ./.
    count = 1
    for i_ in concurrent.futures.as_completed(res):
        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(__index_list) * 100))[0:4] + '%'
        )
        intLogProgress = int(float(count / len(__index_list) * 10000.0))
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(__index_list)),
            ui_log=ui_log
        )
        QA_util_log_info(
            strLogProgress,
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intLogProgress
        )
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS', ui_log=ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        QA_util_log_info(err, ui_log=ui_log)