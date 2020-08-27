
import pymongo
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_etl_financial_TTM
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import DATABASE,QA_util_log_info


def QA_SU_save_fianacialTTM_momgo(client=DATABASE):

    data = QA_util_to_json_from_pandas(QA_util_etl_financial_TTM())
    QA_util_log_info("got financial TTM data.")
    client.drop_collection('financial_TTM')
    col = DATABASE.financial_TTM
    col.create_index(
        [("CODE", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    try:
        col.insert_many(data, ordered=False)
        QA_util_log_info("financial TTM data has been stored imto mongodb.")
    except Exception as e:
        if isinstance(e, MemoryError):
            col.insert_many(data, ordered=True)
        elif isinstance(e, pymongo.bulk.BulkWriteError):
            pass
    pass