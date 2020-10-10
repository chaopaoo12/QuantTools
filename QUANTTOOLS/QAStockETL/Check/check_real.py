from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_alpha191real_adv,QA_fetch_get_stock_half_realtime,QA_fetch_stock_alpha101real_adv
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.Check.check_base import check_stock_base

def QA_fetch_stock_alpha191real(code, start, end):
    return(QA_fetch_stock_alpha191real_adv(code, start, end).data)

def QA_fetch_stock_alpha101real(code, start, end):
    return(QA_fetch_stock_alpha101real_adv(code, start, end).data)

def QA_fetch_stock_half_realtime(code):
    return(QA_fetch_get_stock_half_realtime(code))

def check_stock_alpha191real(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha191real, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock ALPHA191 RealTime', ui_log = ui_log))

def check_stock_alpha101real(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101real, func2=QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock ALPHA101 RealTime', ui_log = ui_log))