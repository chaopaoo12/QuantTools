from QUANTAXIS import QA_fetch_stock_adj,QA_fetch_stock_day_adv,QA_fetch_stock_list_adv,QA_fetch_index_day_adv,QA_fetch_index_list_adv
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade
from QUANTTOOLS.message_func.email import send_email
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import (QA_fetch_stock_fianacial_adv,
                                                           QA_fetch_stock_quant_data_adv,QA_fetch_index_quant_data_adv)

def check_index_day(mark_day = None, type = 'day'):
    index_list = list(QA_fetch_index_list_adv()['code'])

    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)

        #check
        data1 = QA_fetch_index_day_adv(index_list, mark_day, mark_day).data
        data2 = QA_fetch_index_day_adv(index_list, to_date, to_date).data

        #report
        if data1.shape[0] < data2.shape[0]:

            send_email('错误报告', '数据检查错误,指数日线数据', mark_day)
            send_actionnotice('指数日线数据检查错误报告',
                              '指数日线数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass

def check_stock_day(mark_day = None, type = 'day'):
    code = list(QA_fetch_stock_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
        #check
        data1 = QA_fetch_stock_day_adv(code, mark_day, mark_day).data
        data2 = QA_fetch_stock_day_adv(code, to_date, to_date).data
        #report
        if data1.shape[0] < data2.shape[0]:
            send_email('错误报告', '数据检查错误,个股日线数据', mark_day)
            send_actionnotice('个股日线数据检查错误报告',
                              '个股日线数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass

def check_stock_adj(mark_day = None, type = 'day'):
    code = list(QA_fetch_stock_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
        #check
        data1 = QA_fetch_stock_adj(code, mark_day, mark_day)
        data2 = QA_fetch_stock_adj(code, to_date, to_date)
        #report
        if data1.shape[0] < data2.shape[0]:
            send_email('错误报告', '数据检查错误,复权数据', mark_day)
            send_actionnotice('复权数据检查错误报告',
                              '复权数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass

def check_stock_fianacial(mark_day = None, type = 'day'):
    code = list(QA_fetch_stock_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
        #check
        try:
            data1 = QA_fetch_stock_fianacial_adv(code, mark_day, mark_day).data
            data2 = QA_fetch_stock_fianacial_adv(code, to_date, to_date).data
        #report
        except:
            data1 = None
            data2 = None

        if data1 is None:
            send_actionnotice('财分数据检查错误报告',
                              '复权数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = 0),
                              volume= '缺失全部数据'
                              )
        elif data1.shape[0] < data2.shape[0]:
            send_email('错误报告', '数据检查错误,财分数据', mark_day)
            send_actionnotice('财分数据检查错误报告',
                              '财分数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass

def check_stock_quant(mark_day = None, type = 'day'):
    code = list(QA_fetch_stock_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
        #check
        try:
            data1 = QA_fetch_stock_quant_data_adv(code, mark_day, mark_day).data
            data2 = QA_fetch_stock_quant_data_adv(code, to_date, to_date).data
        #report
        except:
            data1 = None
            data2 = None

        if data1 is None:
            send_actionnotice('个股量化数据检查错误报告',
                              '个股量化数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = 0),
                              volume= '缺失全部数据'
                              )
        elif data1.shape[0] < data2.shape[0]:
            send_email('错误报告', '数据检查错误,个股量化数据', mark_day)
            send_actionnotice('个股量化数据检查错误报告',
                              '个股量化数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass

def check_index_quant(mark_day = None, type = 'day'):
    code = list(QA_fetch_index_list_adv()['code'])
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        if QA_util_get_last_day(mark_day) == 'wrong date':
            to_date = QA_util_get_real_date(mark_day)
        else:
            to_date = QA_util_get_last_day(mark_day)
        #check
        try:
            data1 = QA_fetch_index_quant_data_adv(code, mark_day, mark_day).data
            data2 = QA_fetch_index_quant_data_adv(code, to_date, to_date).data
        #report
        except:
            data1 = None
            data2 = None

        if data1 is None:
            send_actionnotice('指数量化数据检查错误报告',
                              '指数量化数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = 0),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失全部数据'
                              )
        elif data1.shape[0] < data2.shape[0]:
            send_email('错误报告', '数据检查错误,指数量化数据', mark_day)
            send_actionnotice('指数量化数据检查错误报告',
                              '指数量化数据缺失:{}'.format(mark_day),
                              'WARNING',
                              direction = '{mark_day}, 数据量:{num}'.format(mark_day = mark_day, num = data1.shape[0]),
                              offset='{to_date}, 数据量:{num}'.format(to_date = to_date, num = data2.shape[0]),
                              volume= '缺失数据量:{num}'.format(num =(data2.shape[0] - data1.shape[0]))
                              )
        else:
            pass