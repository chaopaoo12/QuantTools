from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_datetime_to_strdate)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_interest_rate
import pymongo
import gc

def QA_SU_save_interest_rate(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''

    interest_rate = client.interest_rate
    interest_rate.create_index([("date_stamp", pymongo.ASCENDING)], unique=True)

    def __saving_work(interest_rate):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving interest_rate==== {}'.format(ui_log))

            interest_rate.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_interest_rate()), ordered=False)
            gc.collect()
        except Exception as error0:
            print(error0)
            #err.append(str(code))
    __saving_work( interest_rate)

if __name__ == '__main__':
    pass