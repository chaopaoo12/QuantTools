from QUANTAXIS import QA_fetch_stock_adj,QA_fetch_stock_day_adv,QA_fetch_stock_list_adv,QA_fetch_index_day_adv,QA_fetch_index_list_adv
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_log_info
from QUANTTOOLS.message_func.email import send_email
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,
                                                           QA_fetch_stock_quant_data_adv,
                                                           QA_fetch_index_quant_data_adv,
                                                           QA_fetch_stock_financial_percent_adv,
                                                           QA_fetch_stock_alpha_adv,
                                                           QA_fetch_stock_alpha101_adv,
                                                           QA_fetch_stock_technical_index_adv,
                                                           QA_fetch_index_alpha_adv,
                                                           QA_fetch_index_alpha101_adv,
                                                           QA_fetch_index_technical_index_adv
                                                           )

def check_stock_adj(mark_day = None, type = 'day', ui_log = None):
    code = list(QA_fetch_stock_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
    else:
        mark_day = QA_util_get_real_date(mark_day)
        to_date = QA_util_get_last_day(mark_day)

    #check
    try:
        data1 = QA_fetch_stock_adj(code, mark_day, mark_day)
    #report
    except:
        data1 = None

    try:
        data2 = QA_fetch_stock_adj(code, to_date, to_date)
    #report
    except:
        data2 = None

    #report
    if data1 is None:
        QA_util_log_info(
            '##JOB Now Check Stock adj day data Failed ============== {deal_date} to {to_date} '.format(deal_date=mark_day,
                                                                                                    to_date=to_date), ui_log)
        send_actionnotice('复权数据检查错误报告',
                          '复权数据缺失:{}'.format(mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = 0),
                          volume= '缺失全部数据'
                          )
        return(None)
    elif data1.shape[0] < data2.shape[0]:
        QA_util_log_info(
            '##JOB Now Check Stock adj day data ============== {deal_date}: {num1} to {to_date}: {num2} '.format(deal_date=mark_day,num1=data1.shape[0],
                                                                                                             to_date=to_date,num2=data2.shape[0]), ui_log)
        send_email('错误报告', '数据检查错误,复权数据', mark_day)
        send_actionnotice('复权数据检查错误报告',
                          '复权数据缺失:{}'.format(mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                          volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                          )
        return((data2.shape[0] - data1.shape[0]))
    else:
        return(0)

def check_stock_data(func = None, mark_day = None, title = None, ui_log = None):
    code = list(QA_fetch_stock_list_adv()['code'])
    if mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
    else:
        mark_day = QA_util_get_real_date(mark_day)
        to_date = QA_util_get_last_day(mark_day)

    #check
    try:
        data1 = func(code, mark_day, mark_day).data
    #report
    except:
        data1 = None

    try:
        data2 = func(code, to_date, to_date).data
    #report
    except:
        data2 = None

    if data1 is None:
        QA_util_log_info(
            '##JOB Now Check {title} Failed ============== {deal_date} to {to_date} '.format(title = title,
                                                                                             deal_date=mark_day,
                                                                                             to_date=to_date), ui_log)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}数据缺失:{deal_date}'.format(title = title, deal_date=mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = 0),
                          volume= '缺失全部数据')
        return(None)
    elif data1.shape[0] < data2.shape[0]:
        QA_util_log_info(
            '##JOB Now Check {title} ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                      deal_date=mark_day,
                                                                                                      num1=data1.shape[0],
                                                                                                      to_date=to_date,
                                                                                                      num2=data2.shape[0]), ui_log)
        send_email('错误报告', '数据检查错误,{title}数据'.format(title = title), mark_day)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}据缺失:{mark_day}'.format(title = title,mark_day = mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                          volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0])))
        return((data2.shape[0] - data1.shape[0]))
    else:
        return(0)

def check_index_data(func = None, mark_day = None, title = None, ui_log = None):
    code = list(QA_fetch_index_list_adv()['code'])
    if mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
    else:
        mark_day = QA_util_get_real_date(mark_day)
        to_date = QA_util_get_last_day(mark_day)

    #check
    try:
        data1 = func(code, mark_day, mark_day).data
    #report
    except:
        data1 = None

    try:
        data2 = func(code, to_date, to_date).data
    #report
    except:
        data2 = None

    if data1 is None:
        QA_util_log_info(
            '##JOB Now Check {title} Failed ============== {deal_date} to {to_date} '.format(title = title,
                                                                                             deal_date=mark_day,
                                                                                             to_date=to_date), ui_log)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}数据缺失:{deal_date}'.format(title = title, deal_date=mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = 0),
                          volume= '缺失全部数据')
        return(None)
    elif data1.shape[0] < data2.shape[0]:
        QA_util_log_info(
            '##JOB Now Check {title} ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                      deal_date=mark_day,
                                                                                                      num1=data1.shape[0],
                                                                                                      to_date=to_date,
                                                                                                      num2=data2.shape[0]), ui_log)
        send_email('错误报告', '数据检查错误,{title}数据'.format(title = title),mark_day)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}据缺失:{mark_day}'.format(title = title,mark_day = mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                          volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0])))
        return((data2.shape[0] - data1.shape[0]))
    else:
        return(0)



def check_stock_day(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_day_adv, mark_day = mark_day, title = 'Stock Day', ui_log = ui_log)

def check_stock_fianacial(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_fianacial_adv, mark_day = mark_day, title = 'Stock Finance', ui_log = ui_log)

def check_stock_finper(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_financial_percent_adv, mark_day = mark_day, title = 'PE水位', ui_log = ui_log)

def check_stock_alpha191(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_alpha_adv, mark_day = mark_day, title = 'Stock Alpha191', ui_log = ui_log)

def check_stock_alpha101(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_alpha101_adv, mark_day = mark_day, title = 'Stock Alpha101', ui_log = ui_log)

def QA_fetch_stock_techindex_adv(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='day'))

def check_stock_techindex(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_techindex_adv, mark_day = mark_day, title = 'Stock TechIndex', ui_log = ui_log)

def QA_fetch_stock_techweek_adv(code, start, end):
    return(QA_fetch_stock_technical_index_adv(code, start, end, type='week'))

def check_stock_techweek(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_techweek_adv, mark_day = mark_day, title = 'Stock TechWeek', ui_log = ui_log)

def check_stock_quant(mark_day = None, ui_log = None):
    check_stock_data(func = QA_fetch_stock_quant_data_adv, mark_day = mark_day, title = 'Stock Quant', ui_log = ui_log)


def check_index_day(mark_day = None, ui_log = None):
    check_index_data(func = QA_fetch_index_day_adv, mark_day = mark_day, title = 'Index Day', ui_log = ui_log)

def check_index_alpha191(mark_day = None, ui_log = None):
    check_index_data(func = QA_fetch_index_alpha_adv, mark_day = mark_day, title = 'Index Alpha191', ui_log = ui_log)

def check_index_alpha101(mark_day = None, ui_log = None):
    check_index_data(func = QA_fetch_index_alpha101_adv, mark_day = mark_day, title = 'Index Alpha101', ui_log = ui_log)

def QA_fetch_index_techindex_adv(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='day'))

def check_index_techindex(mark_day = None,  ui_log = None):
    check_index_data(func = QA_fetch_index_techindex_adv, mark_day = mark_day, title = 'Index TechIndex', ui_log = ui_log)

def QA_fetch_index_techweek_adv(code, start, end):
    return(QA_fetch_index_technical_index_adv(code, start, end, type='week'))

def check_index_techweek(mark_day = None, ui_log = None):
    check_index_data(func = QA_fetch_index_techweek_adv, mark_day = mark_day, title = 'Index TechWeek', ui_log = ui_log)

def check_index_quant(mark_day = None, ui_log = None):
    check_index_data(func = QA_fetch_index_quant_data_adv, mark_day = mark_day, title = 'Index Quant', ui_log = ui_log)