from QUANTAXIS import QA_fetch_stock_adj,QA_fetch_index_list_adv
from QUANTAXIS import QA_fetch_get_index_list
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice

def check_stock_base(func1 = None, func2 = None, mark_day = None, title = None, ui_log = None):
    code = list(QA_fetch_stock_all()['code'])

    if mark_day is None:
        mark_day = QA_util_today_str()

    #check
    try:
        try:
            data = func1(code, mark_day, mark_day)
            data1 = data.reset_index().code.unique()
        except:
            data1 = data.code.unique()
    #report
    except:
        data1 = None

    try:
        try:
            data = func2(code, mark_day, mark_day)
            data2 = data.reset_index().code.unique()
        except:
            data2 = data.code.unique()
    #report
    except:
        data2 = None

    if data1 is None:
        QA_util_log_info(
            '##JOB Now Check {title} Failed ============== {deal_date} to {to_date} '.format(title = title,
                                                                                             deal_date=func1.__name__,
                                                                                             to_date=func2.__name__), ui_log)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}数据缺失:{deal_date}'.format(title = title, deal_date=mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = func1.__name__, num = 0),
                          offset='{to_date}, 数据量:{num}'.format(to_date = func2.__name__, num = 0),
                          volume= '缺失全部数据')
        return(None)
    elif len(data1) < len(data2):
        QA_util_log_info(
            '##JOB Now Check {title} ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                      deal_date=func1.__name__,
                                                                                                      num1=len(data1),
                                                                                                      to_date=func2.__name__,
                                                                                                      num2=len(data2)), ui_log)
        #send_email('错误报告', '数据检查错误,{title}数据'.format(title = title), mark_day)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}据缺失:{mark_day}'.format(title = title,mark_day = mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = func2.__name__, num = len(data1)),
                          offset='{to_date}, 数据量:{num}'.format(to_date = func2.__name__, num = len(data2)),
                          volume= '缺失数据量:{num}'.format(num =(len(data2) - len(data1))))

        return([i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1])
    else:
        QA_util_log_info(
            '##JOB Now Check {title} Success ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                              deal_date=func1.__name__,
                                                                                                              num1=len(data1),
                                                                                                              to_date=func2.__name__,
                                                                                                              num2=len(data2)), ui_log)
        return([[i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1]])

def check_stock_data(func = None, mark_day = None, title = None, ui_log = None):
    code = list(QA_fetch_stock_all()['code'])

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
        try:
            data = func(code, mark_day, mark_day)
            data1 = data.reset_index().code.unique()
        except:
            data1 = data.code.unique()
    #report
    except:
        data1 = None

    try:
        data = func(code, to_date, to_date)
        try:
            data2 = data.reset_index().code.unique()
        except:
            data2 = data.code.unique()
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
    elif len(data1) < len(data2):
        QA_util_log_info(
            '##JOB Now Check {title} ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                      deal_date=mark_day,
                                                                                                      num1=len(data1),
                                                                                                      to_date=to_date,
                                                                                                      num2=len(data2)), ui_log)
        #send_email('错误报告', '数据检查错误,{title}数据'.format(title = title), mark_day)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}据缺失:{mark_day}'.format(title = title,mark_day = mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = len(data1)),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = len(data2)),
                          volume= '缺失数据量:{num}'.format(num =(len(data2) - len(data1))))
        return([i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1])
    else:
        QA_util_log_info(
            '##JOB Now Check {title} Success ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                              deal_date=mark_day,
                                                                                                              num1=len(data1),
                                                                                                              to_date=to_date,
                                                                                                              num2=len(data2)), ui_log)
        return([[i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1]])

def check_index_data(func = None, mark_day = None, title = None, ui_log = None):
    try:
        code = list(QA_fetch_index_list_adv()['code'])
    except:
        code = list(QA_fetch_get_index_list('tdx')['code'])

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
        data1 = func(code, mark_day, mark_day).reset_index().code.unique()
    #report
    except:
        data1 = None

    try:
        data2 = func(code, to_date, to_date).reset_index().code.unique()
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
    elif len(data1) < len(data2):
        QA_util_log_info(
            '##JOB Now Check {title} ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                      deal_date=mark_day,
                                                                                                      num1=len(data1),
                                                                                                      to_date=to_date,
                                                                                                      num2=len(data2)), ui_log)
        #send_email('错误报告', '数据检查错误,{title}数据'.format(title = title),mark_day)
        send_actionnotice('{title}检查错误报告'.format(title = title),
                          '{title}据缺失:{mark_day}'.format(title = title,mark_day = mark_day),
                          'WARNING',
                          direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                          offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                          volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0])))
        return([[i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1]])
    else:
        QA_util_log_info(
            '##JOB Now Check {title} Success ============== {deal_date}: {num1} to {to_date}: {num2} '.format(title = title,
                                                                                                              deal_date=mark_day,
                                                                                                              num1=len(data1),
                                                                                                              to_date=to_date,
                                                                                                              num2=len(data2)), ui_log)
        return([[i for i in data1 if i not in data2],
               [i for i in data2 if i not in data1]])