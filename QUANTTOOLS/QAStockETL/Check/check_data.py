from QUANTAXIS import QA_fetch_stock_day_adv, QA_fetch_index_day_adv,QA_fetch_stock_min_adv, QA_fetch_stock_adj
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,
                                                           QA_fetch_stock_quant_data_adv,
                                                           QA_fetch_index_quant_data_adv,
                                                           QA_fetch_stock_financial_percent_adv,
                                                           QA_fetch_stock_alpha_adv,
                                                           QA_fetch_stock_alpha101_adv,
                                                           QA_fetch_stock_technical_index_adv,
                                                           QA_fetch_index_alpha_adv,
                                                           QA_fetch_index_alpha101_adv,
                                                           QA_fetch_index_technical_index_adv,
                                                           QA_fetch_stock_alpha101half_adv,
                                                           QA_fetch_stock_half_adv,
                                                           QA_fetch_stock_alpha191half_adv
                                                           )
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_half_realtime
from QUANTTOOLS.QAStockETL.Check.check_base import check_stock_data, check_index_data, check_stock_base

def QA_fetch_stock_half_realtime(code, start, end):
    return(QA_fetch_get_stock_half_realtime(code, start))


def QA_fetch_stock_60min(code, start, end):
    return(QA_fetch_stock_min_adv(code, start, end, frequence='60min').data)

def check_stock_60min(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_60min, mark_day = mark_day, title = 'Stock 60Min', ui_log = ui_log))

def check_sinastock_60min(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_60min, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock 60Min sina', ui_log = ui_log))


def QA_fetch_stock_half(code, start, end):
    return(QA_fetch_stock_half_adv(code, start, end).data)

def check_stock_half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_half, mark_day = mark_day, title = 'Stock Half', ui_log = ui_log))

def check_sinastock_half(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_half, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Half sina', ui_log = ui_log))


def QA_fetch_stock_day(code, start, end):
    return(QA_fetch_stock_day_adv(code, start, end).data)

def check_stock_day(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_day, mark_day = mark_day, title = 'Stock Day', ui_log = ui_log))

def check_sinastock_day(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_day, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Day sina', ui_log = ui_log))


def check_stock_adj(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_adj, mark_day = mark_day, title = 'Stock Adj', ui_log = ui_log))

def check_sinastock_adj(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_adj, func2= QA_fetch_stock_day, mark_day = mark_day, title = 'Stock Adj', ui_log = ui_log))


def QA_fetch_stock_fianacial(code, start, end):
    return(QA_fetch_stock_fianacial_adv(code, start, end).data)

def check_stock_fianacial(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_fianacial, mark_day = mark_day, title = 'Stock Finance', ui_log = ui_log))


def QA_fetch_stock_financial_percent(code, start, end):
    return(QA_fetch_stock_financial_percent_adv(code, start, end).data)

def check_stock_finper(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_financial_percent_adv, mark_day = mark_day, title = 'PE水位', ui_log = ui_log))


def QA_fetch_stock_alpha(code, start, end):
    return(QA_fetch_stock_alpha_adv(code, start, end).data)

def check_stock_alpha191(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha_adv, mark_day = mark_day, title = 'Stock Alpha191', ui_log = ui_log))


def QA_fetch_stock_alpha101(code, start, end):
    return(QA_fetch_stock_alpha101_adv(code, start, end).data)

def check_stock_alpha101(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101, mark_day = mark_day, title = 'Stock Alpha101', ui_log = ui_log))

def check_sinastock_alpha101(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Alpha101 Half sina', ui_log = ui_log))


def QA_fetch_stock_alpha101half(code, start, end):
    return(QA_fetch_stock_alpha101half_adv(code, start, end).data)

def check_stock_alpha101half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101half, mark_day = mark_day, title = 'Stock Alpha101 Half', ui_log = ui_log))

def check_sinastock_alpha101half(mark_day = None, ui_log = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101half, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Alpha101 Half sina', ui_log = ui_log))


def QA_fetch_stock_alpha191half(code, start, end):
    return(QA_fetch_stock_alpha191half_adv(code, start, end).data)

def check_stock_alpha191half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha191half, mark_day = mark_day, title = 'Stock Alpha191 Half', ui_log = ui_log))



def QA_fetch_stock_techindex(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='day').data)

def check_stock_techindex(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_techindex, mark_day = mark_day, title = 'Stock TechIndex', ui_log = ui_log))

def QA_fetch_stock_techweek(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='week').data)

def check_stock_techweek(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_techweek, mark_day = mark_day, title = 'Stock TechWeek', ui_log = ui_log))


def QA_fetch_stock_quant_data(code, start, end):
    return(QA_fetch_stock_quant_data_adv(code, start, end).data)

def check_stock_quant(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_quant_data, mark_day = mark_day, title = 'Stock Quant', ui_log = ui_log))


def QA_fetch_index_day(code, start, end):
    return(QA_fetch_index_day_adv(code, start, end).data)

def check_index_day(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_day, mark_day = mark_day, title = 'Index Day', ui_log = ui_log))

def QA_fetch_index_alpha(code, start, end):
    return(QA_fetch_index_alpha_adv(code, start, end).data)

def check_index_alpha191(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_alpha_adv, mark_day = mark_day, title = 'Index Alpha191', ui_log = ui_log))

def QA_fetch_index_alpha101(code, start, end):
    return(QA_fetch_index_alpha101_adv(code, start, end).data)

def check_index_alpha101(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_alpha101, mark_day = mark_day, title = 'Index Alpha101', ui_log = ui_log))


def QA_fetch_index_techindex(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='day').data)

def check_index_techindex(mark_day = None,  ui_log = None):
    return(check_index_data(func = QA_fetch_index_techindex, mark_day = mark_day, title = 'Index TechIndex', ui_log = ui_log))

def QA_fetch_index_techweek(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='week').data)

def check_index_techweek(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_techweek, mark_day = mark_day, title = 'Index TechWeek', ui_log = ui_log))

def QA_fetch_index_quant_data(code, start, end):
    return(QA_fetch_index_quant_data_adv(code, start, end).data)

def check_index_quant(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_quant_data_adv, mark_day = mark_day, title = 'Index Quant', ui_log = ui_log))

if __name__ == 'main':
    pass