from QUANTAXIS import QA_fetch_stock_day_adv, QA_fetch_index_day_adv,QA_fetch_stock_min_adv
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
from QUANTTOOLS.QAStockETL.Check.check_base import check_stock_data, check_index_data

def QA_fetch_stock_60min(code, start, end):
    return(QA_fetch_stock_min_adv(code, start, end, frequence='60min'))

def check_stock_60min(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_60min, mark_day = mark_day, title = 'Stock 60Min', ui_log = ui_log))

def check_stock_half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_half_adv, mark_day = mark_day, title = 'Stock Half', ui_log = ui_log))

def check_stock_day(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_day_adv, mark_day = mark_day, title = 'Stock Day', ui_log = ui_log))

def check_stock_fianacial(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_fianacial_adv, mark_day = mark_day, title = 'Stock Finance', ui_log = ui_log))

def check_stock_finper(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_financial_percent_adv, mark_day = mark_day, title = 'PE水位', ui_log = ui_log))

def check_stock_alpha191(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha_adv, mark_day = mark_day, title = 'Stock Alpha191', ui_log = ui_log))

def check_stock_alpha101(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101_adv, mark_day = mark_day, title = 'Stock Alpha101', ui_log = ui_log))

def check_stock_alpha101half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101half_adv, mark_day = mark_day, title = 'Stock Alpha101 Half', ui_log = ui_log))

def check_stock_alpha191half(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_alpha191half_adv, mark_day = mark_day, title = 'Stock Alpha101 Half', ui_log = ui_log))

def QA_fetch_stock_techindex_adv(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='day'))

def check_stock_techindex(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_techindex_adv, mark_day = mark_day, title = 'Stock TechIndex', ui_log = ui_log))

def QA_fetch_stock_techweek_adv(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='week'))

def check_stock_techweek(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_techweek_adv, mark_day = mark_day, title = 'Stock TechWeek', ui_log = ui_log))

def check_stock_quant(mark_day = None, ui_log = None):
    return(check_stock_data(func = QA_fetch_stock_quant_data_adv, mark_day = mark_day, title = 'Stock Quant', ui_log = ui_log))

def check_index_day(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_day_adv, mark_day = mark_day, title = 'Index Day', ui_log = ui_log))

def check_index_alpha191(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_alpha_adv, mark_day = mark_day, title = 'Index Alpha191', ui_log = ui_log))

def check_index_alpha101(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_alpha101_adv, mark_day = mark_day, title = 'Index Alpha101', ui_log = ui_log))

def QA_fetch_index_techindex_adv(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='day'))

def check_index_techindex(mark_day = None,  ui_log = None):
    return(check_index_data(func = QA_fetch_index_techindex_adv, mark_day = mark_day, title = 'Index TechIndex', ui_log = ui_log))

def QA_fetch_index_techweek_adv(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='week'))

def check_index_techweek(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_techweek_adv, mark_day = mark_day, title = 'Index TechWeek', ui_log = ui_log))

def check_index_quant(mark_day = None, ui_log = None):
    return(check_index_data(func = QA_fetch_index_quant_data_adv, mark_day = mark_day, title = 'Index Quant', ui_log = ui_log))

if __name__ == 'main':
    pass