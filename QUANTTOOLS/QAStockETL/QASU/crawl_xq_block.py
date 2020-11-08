from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_block_day_xq)
from QUANTTOOLS.QAStockETL.QAData.settting import bk, SW
import pymongo
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
import datetime

def now_time():
    return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
           ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
        str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def QA_SU_save_block_xq_day(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''

    stock_list = list(bk.keys()) + list(SW.keys())
    coll_stock_day = client.block_day_xq
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
                '##JOB01 Now Saving BLOCK_XQ_DAY==== {}'.format(str(code)),
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
                    'UPDATE_BLOCK_XQ_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_block_day_xq(str(code),
                                                        QA_util_get_next_day(start_date),
                                                        end_date
                                                        )
                        )
                    )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '2016-01-01'
                QA_util_log_info(
                    'UPDATE_BLOCK_XQ_DAY \n Trying updating {} from {} to {}'
                        .format(code,
                                start_date,
                                end_date),
                    ui_log
                )
                if start_date != end_date:
                    coll_stock_day.insert_many(
                        QA_util_to_json_from_pandas(
                            QA_fetch_get_block_day_xq(
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
        QA_util_log_info('SUCCESS save block xq day^_^', ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)
