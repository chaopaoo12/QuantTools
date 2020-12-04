from QUANTAXIS import QA_fetch_stock_day_adv,QA_fetch_stock_min_adv, QA_fetch_index_day_adv, QA_fetch_index_min_adv, QA_fetch_stock_adj
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
                                                           QA_fetch_stock_alpha191half_adv,
                                                           QA_fetch_stock_technical_half_adv
                                                           )
from QUANTTOOLS.QAStockETL.QAUtil.QASQLStockAlpha191Half import QA_Sql_Stock_Alpha191Half
from QUANTTOOLS.QAStockETL.QAUtil.QASQLStockAlpha101Half import QA_Sql_Stock_Alpha101Half
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_half_realtime,QA_fetch_stock_alpha_real,QA_fetch_stock_alpha101_real
from QUANTTOOLS.QAStockETL.Check.check_base import check_stock_data, check_index_data, check_stock_base

def QA_fetch_stock_half_realtime(code, start, end):
    return(QA_fetch_get_stock_half_realtime(code, start))

def QA_fetch_stock_15min(code, start, end):
    return(QA_fetch_stock_min_adv(code, start, end, frequence='15min').data)

def check_stock_15min(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_60min, mark_day = mark_day, title = 'Stock 15Min'))

def check_sinastock_15min(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_15min, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock 15Min sina'))

def QA_fetch_stock_60min(code, start, end):
    return(QA_fetch_stock_min_adv(code, start, end, frequence='60min').data)

def check_stock_60min(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_60min, mark_day = mark_day, title = 'Stock 60Min'))

def check_sinastock_60min(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_60min, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock 60Min sina'))


def QA_fetch_stock_half(code, start, end):
    return(QA_fetch_stock_half_adv(code, start, end).data)

def check_stock_half(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_half, mark_day = mark_day, title = 'Stock Half'))

def check_sinastock_half(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_half, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Half sina'))


def QA_fetch_stock_day(code, start, end):
    return(QA_fetch_stock_day_adv(code, start, end).data)

def check_stock_day(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_day, mark_day = mark_day, title = 'Stock Day'))

def check_sinastock_day(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_day, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Day sina'))


def check_stock_adj(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_adj, mark_day = mark_day, title = 'Stock Adj'))

def check_sinastock_adj(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_adj, func2= QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Adj Sina'))


def QA_fetch_stock_fianacial(code, start, end):
    return(QA_fetch_stock_fianacial_adv(code, start, end).data)

def check_stock_fianacial(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_fianacial, mark_day = mark_day, title = 'Stock Finance'))


def QA_fetch_stock_financial_percent(code, start, end):
    return(QA_fetch_stock_financial_percent_adv(code, start, end).data)

def check_stock_finper(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_financial_percent, mark_day = mark_day, title = 'PE水位'))


def QA_fetch_stock_alpha(code, start, end):
    return(QA_fetch_stock_alpha_adv(code, start, end).data)

def check_stock_alpha191(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_alpha, mark_day = mark_day, title = 'Stock Alpha191'))

def check_sinastock_alpha191(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock Alpha191 sina'))


def QA_fetch_stock_alpha101(code, start, end):
    return(QA_fetch_stock_alpha101_adv(code, start, end).data)

def check_stock_alpha101(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101, mark_day = mark_day, title = 'Stock Alpha101'))

def check_sinastock_alpha101(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101, func2 = QA_fetch_stock_alpha101_real, mark_day = mark_day, title = 'Stock Alpha101 sina'))


def QA_fetch_stock_alpha101half(code, start, end):
    return(QA_fetch_stock_alpha101half_adv(code, start, end).data)

def check_stock_alpha101half(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_alpha101half, mark_day = mark_day, title = 'Stock Alpha101 Half'))

def check_sinastock_alpha101half(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101half, func2 = QA_fetch_stock_alpha_real, mark_day = mark_day, title = 'Stock Alpha101 Half sina'))

def QA_fetch_stock_alpha101_real_model(code, start, end):
    return(QA_Sql_Stock_Alpha101Half(start, end))

def check_realstock_alpha101half(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha101_real_model, func2 = QA_fetch_stock_alpha101_real, mark_day = mark_day, title = 'Stock Alpha101 Half Real'))


def QA_fetch_stock_alpha191half(code, start, end):
    return(QA_fetch_stock_alpha191half_adv(code, start, end).data)

def check_stock_alpha191half(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_alpha191half, mark_day = mark_day, title = 'Stock Alpha191 Half'))

def check_sinastock_alpha191half(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha191half, func2 = QA_fetch_stock_alpha_real, mark_day = mark_day, title = 'Stock Alpha191 Half sina'))

def QA_fetch_stock_alpha191_real_model(code, start, end):
    return(QA_Sql_Stock_Alpha191Half(start, end))

def check_realstock_alpha191half(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_alpha191_real_model, func2 = QA_fetch_stock_alpha_real, mark_day = mark_day, title = 'Stock Alpha191 Half Real'))

def QA_fetch_stock_tech15min(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='15min').data)

def check_stock_tech15min(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_tech15min, mark_day = mark_day, title = 'Stock Tech15min'))

def QA_fetch_stock_techhour(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='hour').data)

def check_stock_techhour(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_techhour, mark_day = mark_day, title = 'Stock TechHour'))

def QA_fetch_stock_techindex(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='day').data)

def check_stock_techindex(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_techindex, mark_day = mark_day, title = 'Stock TechIndex'))

def check_sinastock_techindex(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_techindex, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock TechIndex sina'))

def QA_fetch_stock_techhalf(code, start, end):
    return(QA_fetch_stock_technical_half_adv(code, start, end, type='day').data)

def check_sinastock_tech(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_techhalf, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock TechHalf sina'))


def QA_fetch_stock_techweek(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='week').data)

def check_stock_techweek(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_techweek, mark_day = mark_day, title = 'Stock TechWeek'))

def check_sinastock_techweek(mark_day = None):
    return(check_stock_base(func1 = QA_fetch_stock_techweek, func2 = QA_fetch_stock_half_realtime, mark_day = mark_day, title = 'Stock TechWeek sina'))


def QA_fetch_stock_quant_data(code, start, end):
    return(QA_fetch_stock_quant_data_adv(code, start, end).data)

def check_stock_quant(mark_day = None):
    return(check_stock_data(func = QA_fetch_stock_quant_data, mark_day = mark_day, title = 'Stock Quant'))

def QA_fetch_index_60min(code, start, end):
    return(QA_fetch_index_min_adv(code, start, end, frequence='60min').data)

def check_index_60min(mark_day = None):
    return(check_index_data(func = QA_fetch_index_60min, mark_day = mark_day, title = 'Index 60Min'))

def QA_fetch_index_day(code, start, end):
    return(QA_fetch_index_day_adv(code, start, end).data)

def check_index_day(mark_day = None):
    return(check_index_data(func = QA_fetch_index_day, mark_day = mark_day, title = 'Index Day'))

def QA_fetch_index_alpha(code, start, end):
    return(QA_fetch_index_alpha_adv(code, start, end).data)

def check_index_alpha191(mark_day = None):
    return(check_index_data(func = QA_fetch_index_alpha_adv, mark_day = mark_day, title = 'Index Alpha191'))

def QA_fetch_index_alpha101(code, start, end):
    return(QA_fetch_index_alpha101_adv(code, start, end).data)

def check_index_alpha101(mark_day = None):
    return(check_index_data(func = QA_fetch_index_alpha101, mark_day = mark_day, title = 'Index Alpha101'))

def QA_fetch_index_techhour(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='hour').data)

def check_index_techhour(mark_day = None):
    return(check_index_data(func = QA_fetch_index_techhour, mark_day = mark_day, title = 'Index TechHour'))

def QA_fetch_index_techindex(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='day').data)

def check_index_techindex(mark_day = None):
    return(check_index_data(func = QA_fetch_index_techindex, mark_day = mark_day, title = 'Index TechIndex'))

def QA_fetch_index_techweek(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='week').data)

def check_index_techweek(mark_day = None):
    return(check_index_data(func = QA_fetch_index_techweek, mark_day = mark_day, title = 'Index TechWeek'))

def QA_fetch_index_quant_data(code, start, end):
    return(QA_fetch_index_quant_data_adv(code, start, end).data)

def check_index_quant(mark_day = None):
    return(check_index_data(func = QA_fetch_index_quant_data_adv, mark_day = mark_day, title = 'Index Quant'))

if __name__ == 'main':
    pass