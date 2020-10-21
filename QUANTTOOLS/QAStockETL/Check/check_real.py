from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_stock_alpha191real_adv,QA_fetch_get_stock_half_realtime,
                                           QA_fetch_stock_alpha101real_adv,QA_fetch_stock_real,
                                           QA_fetch_stock_technical_half_adv)
from QUANTTOOLS.QAStockETL.Check.check_base import check_stock_base

def QA_fetch_stock_alpha191real(code, start, end):
    return(QA_fetch_stock_alpha191real_adv(code, start, end).data)

def QA_fetch_stock_alpha101real(code, start, end):
    return(QA_fetch_stock_alpha101real_adv(code, start, end).data)

def QA_fetch_stock_techreal(code, start, end):
    return(QA_fetch_stock_technical_half_adv(code, start, end).data)

def QA_fetch_stock_half_realtime(code, start, end):
    return(QA_fetch_get_stock_half_realtime(code))

def check_stock_alpha191real(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha191real, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock ALPHA191 RealTime'))

def check_stock_alpha101real(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101real, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock ALPHA101 RealTime'))

def check_stock_techreal(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_techreal, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock TechHalf RealTime'))

def check_stock_real(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_real, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Day RealTime'))