from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_datetime_to_strdate)
from QUANTTOOLS.QAStockETL.QAFetch.QAhkstock import QA_fetch_get_usstock_list_sina
import pymongo
import gc

def QA_SU_save_usstock_list_day(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    sina_usstock = client.sina_usstock
    sina_usstock.create_index([("code", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(sina_usstock):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving SINA US Stock List ==== '.format(ui_log))

            sina_usstock.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_usstock_list_sina()), ordered=False)
            gc.collect()
        except Exception as error0:
            print(error0)
            err.append()

    __saving_work(sina_usstock)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save SINA US Stock List ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)
