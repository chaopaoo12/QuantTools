
import datetime
import pandas as pd

from QUANTAXIS.QAData import (QA_DataStruct_Financial,
                              QA_DataStruct_Stock_day)

from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import (QA_fetch_financial_report, QA_fetch_stock_financial_calendar,
                                                   QA_fetch_stock_divyield,
                                                   QA_fetch_financial_TTM,
                                                   QA_fetch_stock_fianacial)

from QUANTAXIS.QAUtil.QADate import month_data
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter,
                              QA_util_datetime_to_strdate, QA_util_add_months,
                              QA_util_today_str)

def QA_fetch_financial_report_adv(code, start='all', end=None, type='report'):
    """高级财务查询接口

    Arguments:
        code {[type]} -- [description]
        start {[type]} -- [description]

    Keyword Arguments:
        end {[type]} -- [description] (default: {None})
    """
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = QA_util_today_str()
    if end is None:
        end = str(datetime.date.today())
        date_list = list(pd.DataFrame.from_dict(QA_util_getBetweenQuarter(
            start, QA_util_datetime_to_strdate(QA_util_add_months(end, -3)))).T.iloc[:, 1])
        if type == 'report':
            return QA_DataStruct_Financial(QA_fetch_financial_report(code, date_list))
        elif type == 'date':
            return QA_DataStruct_Financial(QA_fetch_financial_report(code, date_list, type='date'))
    else:
        daterange = pd.date_range(start, end)
        timerange = [item.strftime('%Y-%m-%d') for item in list(daterange)]
        if type == 'report':
            return QA_DataStruct_Financial(QA_fetch_financial_report(code, timerange))
        elif type == 'date':
            return QA_DataStruct_Financial(QA_fetch_financial_report(code, timerange, type='date'))


def QA_fetch_stock_financial_calendar_adv(code, start="all", end=None, format='pd', collections=DATABASE.report_calendar):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all':
        start = '1990-01-01'
        end = QA_util_today_str()

    if end is None:

        return QA_DataStruct_Financial(QA_fetch_stock_financial_calendar(code, start, str(datetime.date.today())))
    else:
        series = pd.Series(
            data=month_data, index=pd.to_datetime(month_data), name='date')
        timerange = series.loc[start:end].tolist()
        return QA_DataStruct_Financial(QA_fetch_stock_financial_calendar(code, start, end))


def QA_fetch_stock_divyield_adv(code, start="all", end=None, format='pd', collections=DATABASE.report_calendar):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    if end is None:

        return QA_DataStruct_Financial(QA_fetch_stock_divyield(code, start, str(datetime.date.today())))
    else:
        series = pd.Series(
            data=month_data, index=pd.to_datetime(month_data), name='date')
        timerange = series.loc[start:end].tolist()
        return QA_DataStruct_Financial(QA_fetch_stock_divyield(code, start, end))

def QA_fetch_financial_TTM_adv(code, start="all", end=None, format='pd', collections=DATABASE.financial_TTM):
    '获取财报TTM'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all':
        start = '2001-01-01'
        end = QA_util_today_str()

    if end is None:
        return QA_DataStruct_Financial(QA_fetch_financial_TTM(code, start, str(datetime.date.today())))
    else:
        series = pd.Series(
            data=month_data, index=pd.to_datetime(month_data), name='date')
        timerange = series.loc[start:end].tolist()
        return QA_DataStruct_Financial(QA_fetch_financial_TTM(code, start, end))

def QA_fetch_stock_fianacial_adv(code,
                                 start='all', end=None,
                                 if_drop_index=True,):
    '获取财报TTM'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = QA_util_today_str()

    res = QA_fetch_stock_fianacial(code, start, end, format='pd')
    if res is None:
        #  todo 报告是代码不合法，还是日期不合法
        print("QA Error QA_fetch_stock_fianacial_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_fianacial_adv return None" % (
            code, start, end))
        return None
    else:
        res_reset_index = res.set_index(['DATE', 'CODE'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_fianacial_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_Stock_day(res_reset_index)