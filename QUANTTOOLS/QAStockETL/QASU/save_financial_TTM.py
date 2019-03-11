
import pymongo
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_etl_financial_TTM
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import DATABASE


def QA_SU_save_fianacialTTM_momgo():

    data = QA_util_to_json_from_pandas(QA_util_etl_financial_TTM())
    print("got financial TTM data.")
    col = DATABASE.financial_TTM
    col.create_index(
        [("CODE", ASCENDING), ("REPORT_DATE", ASCENDING)], unique=True)
    try:
        col.insert_many(data, ordered=False)
        print("financial TTM data has been stored imto mongodb.")
    except Exception as e:
        if isinstance(e, MemoryError):
            col.insert_many(data, ordered=True)
        elif isinstance(e, pymongo.bulk.BulkWriteError):
            pass
    pass