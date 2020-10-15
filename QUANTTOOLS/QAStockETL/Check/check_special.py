from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_financial_code_wy,QA_fetch_financial_code_ttm,
                                           QA_fetch_financial_code_tdx)

def check_ttm_financial(mark_day=None, type='day', ui_log = None):
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    data = QA_fetch_financial_code_ttm()
    res = data[data.real_date < mark_day]
    if res is None or res.shape[0] == 0:
        QA_util_log_info(
            '##JOB Now Check TTM Financial Reports data Success ============== {deal_date}'.format(deal_date=mark_day), ui_log)
        return(0)
    else:
        QA_util_log_info(res)
        QA_util_log_info(
            '##JOB Now Check TTM Financial Reports data Missing ============== {deal_date}: {num} Reports  '.format(deal_date=mark_day,num=res.shape[0]), ui_log)
        #send_email('错误报告', '数据检查错误,复权数据', mark_day)
        send_actionnotice('TTM财报数据检查错误报告',
                          'TTM财报数据缺失:{}'.format(mark_day),
                          'WARNING',
                          direction = 'Missing Data',
                          offset='None',
                          volume= '缺失数据量:{num}'.format(num =(res.shape[0]))
                          )
        return(None)

def check_tdx_financial(mark_day=None, type='day', ui_log = None):
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    data = QA_fetch_financial_code_tdx()
    res = data[data.real_date < mark_day]
    if res is None or res.shape[0] == 0:
        QA_util_log_info(
            '##JOB Now Check TDX Financial Reports data Success ============== {deal_date}'.format(deal_date=mark_day), ui_log)
        return(0)
    else:
        QA_util_log_info(res)
        QA_util_log_info(
            '##JOB Now Check TDX Financial Reports data Missing ============== {deal_date}: {num} Reports  '.format(deal_date=mark_day,num=res.shape[0]), ui_log)
        #send_email('错误报告', '数据检查错误,复权数据', mark_day)
        send_actionnotice('TDX财报数据检查错误报告',
                          'TDX财报数据缺失:{}'.format(mark_day),
                          'WARNING',
                          direction = 'Missing Data',
                          offset='None',
                          volume= '缺失数据量:{num}'.format(num =(res.shape[0]))
                          )
        return(None)

def check_wy_financial(mark_day=None, type='day', ui_log = None):
    if type == 'day' and mark_day is None:
        mark_day = QA_util_today_str()

    data = QA_fetch_financial_code_wy()
    res = data[data.real_date < mark_day]
    if res is None or res.shape[0] == 0:
        QA_util_log_info(
            '##JOB Now Check WY Financial Reports data Success ============== {deal_date}'.format(deal_date=mark_day), ui_log)
        return(0)
    else:
        QA_util_log_info(res)
        QA_util_log_info(
            '##JOB Now Check WY Financial Reports data Missing ============== {deal_date}: {num} Reports'.format(deal_date=mark_day,num=res.shape[0]), ui_log)
        #send_email('错误报告', '数据检查错误,复权数据', mark_day)
        send_actionnotice('网易财报数据检查错误报告',
                          '网易财报数据缺失:{}'.format(mark_day),
                          'WARNING',
                          direction = 'Missing Data',
                          offset='None',
                          volume= '缺失数据量:{num}'.format(num =(res.shape[0]))
                          )
        return(None)

