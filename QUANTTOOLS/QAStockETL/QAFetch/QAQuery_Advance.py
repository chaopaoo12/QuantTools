
import datetime
import pandas as pd

from QUANTAXIS.QAData import (QA_DataStruct_Financial,
                              QA_DataStruct_Stock_day,
                              QA_DataStruct_Index_day)
from QUANTTOOLS.QAStockETL.QAData.QA_DataStruct_UsStock_day import (
                              QA_DataStruct_UsStock_day
                              )
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery import (QA_fetch_financial_report,
                                                   QA_fetch_stock_financial_calendar,
                                                   QA_fetch_stock_divyield,
                                                   QA_fetch_financial_TTM,
                                                   QA_fetch_stock_fianacial,
                                                   QA_fetch_stock_alpha,
                                                   QA_fetch_stock_alpha101,
                                                   QA_fetch_stock_shares,
                                                   QA_fetch_financial_report_wy,
                                                   QA_fetch_stock_technical_index,
                                                   QA_fetch_stock_financial_percent,
                                                   QA_fetch_stock_quant_data,
                                                   QA_fetch_stock_quant_pre,
                                                   QA_fetch_stock_quant_pre_train,
                                                   QA_fetch_stock_target,
                                                   QA_fetch_interest_rate,
                                                   QA_fetch_index_alpha,
                                                   QA_fetch_index_alpha101,
                                                   QA_fetch_index_technical_index,
                                                   QA_fetch_index_quant_data,
                                                   QA_fetch_index_quant_pre,
                                                   QA_fetch_stock_week,
                                                   QA_fetch_stock_month,
                                                   QA_fetch_stock_year,
                                                   QA_fetch_index_week,
                                                   QA_fetch_index_month,
                                                   QA_fetch_index_year,
                                                   QA_fetch_stock_alpha101half,
                                                   QA_fetch_stock_half,
                                                   QA_fetch_stock_alpha191half,
                                                   QA_fetch_usstock_day,
                                                   QA_fetch_stock_alpha_real,
                                                   QA_fetch_stock_alpha101_real,
                                                   QA_fetch_usstock_xq_day,
                                                   QA_fetch_stock_technical_half,
                                                   QA_fetch_usstock_alpha,
                                                   QA_fetch_usstock_alpha101,
                                                   QA_fetch_usstock_technical_index,
                                                   QA_fetch_usstock_financial_percent,
                                                   QA_fetch_stock_base_real)
from QUANTAXIS.QAUtil.QADate import month_data
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter,QA_util_log_info,
                              QA_util_datetime_to_strdate, QA_util_add_months,
                              QA_util_today_str)

def QA_fetch_financial_report_adv(code=None, start=None, end=None, type='report', ltype='EN'):
    """高级财务查询接口
    Arguments:
        code {[type]} -- [description]
        start {[type]} -- [description]
    Keyword Arguments:
        end {[type]} -- [description] (default: {None})
    """
    return QA_DataStruct_Financial(QA_fetch_financial_report(code, start, end, type=type, ltype=ltype))


def QA_fetch_stock_financial_calendar_adv(code, start="all", end=None, type='day', collections=DATABASE.report_calendar):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    # code checking
    if start == 'all':
        start = '2007-01-01'
        end = QA_util_today_str()
    if end is None:
        end = QA_util_today_str()

    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    return QA_DataStruct_Financial(QA_fetch_stock_financial_calendar(code, start, end, type=type, format='pd'))


def QA_fetch_stock_divyield_adv(code, start="all", end=None,type='crawl', collections=DATABASE.stock_divyield):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all':
        start = '2007-01-01'
        end = QA_util_today_str()
    if end is None:
        end = QA_util_today_str()
    return QA_DataStruct_Financial(QA_fetch_stock_divyield(code, start, end, type =type, format='pd'))

def QA_fetch_financial_TTM_adv(code, start="all", end=None, collections=DATABASE.financial_TTM):
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
        return QA_DataStruct_Financial(QA_fetch_financial_TTM(code, start, str(datetime.date.today()), format='pd'))
    else:
        series = pd.Series(
            data=month_data, index=pd.to_datetime(month_data), name='date')
        timerange = series.loc[start:end].tolist()
        return QA_DataStruct_Financial(QA_fetch_financial_TTM(code, start, end, format='pd'))

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
        QA_util_log_info("QA Error QA_fetch_stock_fianacial_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_fianacial_adv return None" % (
            code, start, end))
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_fianacial_adv set index 'datetime, code' return None")
        #     return
        return QA_DataStruct_Financial(res_reset_index)

def QA_fetch_stock_alpha_adv(code, start="all", end=None, collections=DATABASE.stock_alpha):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_shares_adv(code, start="all", end=None,type='crawl', collections=DATABASE.stock_shares):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    # code checking
    if start == 'all':
        start = '2001-01-01'
    if end is None:
        end = QA_util_today_str()

    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]
    return QA_DataStruct_Financial(QA_fetch_stock_shares(code, start, end, type =type, format='pd'))

def QA_fetch_financial_report_wy_adv(code=None, start=None, end=None, type='report', ltype='EN'):
    """高级财务查询接口
    Arguments:
        code {[type]} -- [description]
        start {[type]} -- [description]
    Keyword Arguments:
        end {[type]} -- [description] (default: {None})
    """
    return QA_DataStruct_Financial(QA_fetch_financial_report_wy(code, start, end, type=type, ltype=ltype))

def QA_fetch_stock_technical_index_adv(code, start="all", end=None, type='day', collections=DATABASE.stock_technical_index):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_financial_percent_adv(code, start="all", end=None, collections=DATABASE.stock_financial_percent):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_financial_percent(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_financial_percent(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_quant_data_adv(code, start="all", end=None, block=True, collections=DATABASE.stock_quant_data):
    '获取股票量化机器学习最终指标V1'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_quant_data(code, start, end, block, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_quant_data(code, start, end, block, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_quant_pre_adv(code, start="all", end=None, block=True, type='close', method= 'value', norm_type='normalization'):
    '获取股票量化机器学习数据查询接口'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_quant_pre(code, start, end, block, type=type, method=method, norm_type=norm_type, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_quant_pre(code, start, end, block, type=type, method=method, norm_type=norm_type, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_quant_pre_train_adv(code, start="all", end=None, block=True, type='close', method= 'value', norm_type='normalization'):
    '获取股票量化机器学习数据查询接口'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_quant_pre_train(code, start, end, block, type=type, method=method, norm_type=norm_type, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_quant_pre_train(code, start, end, block, type=type, method=method, norm_type=norm_type, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_target_adv(code, start="all", end=None, type='close', method= 'value'):
    '获取股票量化机器学习数据查询接口'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_target(code, start, end, type=type, method=method)
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_target(code, start, end, type=type, method=method)
        return QA_DataStruct_Financial(data)

def QA_fetch_interest_rate_adv(start="all", end=None):
    '获取股票日线'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all':
        start = '1991-01-01'
        end = QA_util_today_str()
    if end is None:
        end = QA_util_today_str()
    return QA_fetch_interest_rate(start, end, format='pd')

def QA_fetch_index_alpha_adv(code, start="all", end=None, collections=DATABASE.index_alpha):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_index_alpha(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)
    else:
        data = QA_fetch_index_alpha(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)


def QA_fetch_index_technical_index_adv(code, start="all", end=None, type='day', collections=DATABASE.index_technical_index):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_index_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Index_day(data)
    else:
        data = QA_fetch_index_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Index_day(data)

def QA_fetch_index_quant_data_adv(code, start="all", end=None, collections=DATABASE.stock_quant_data):
    '获取股票量化机器学习最终指标V1'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_index_quant_data(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)
    else:
        data = QA_fetch_index_quant_data(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)

def QA_fetch_index_quant_pre_adv(code, start="all", end=None, method='value'):
    '获取股票量化机器学习数据查询接口'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_index_quant_pre(code, start, end, method=method, format='pd')
        return QA_DataStruct_Index_day(data)
    else:
        data = QA_fetch_index_quant_pre(code, start, end, method=method, format='pd')
        return QA_DataStruct_Index_day(data)

def QA_fetch_stock_alpha101_adv(code, start="all", end=None, collections=DATABASE.stock_alpha):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_index_alpha101_adv(code, start="all", end=None, collections=DATABASE.index_alpha):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_index_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)
    else:
        data = QA_fetch_index_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Index_day(data)

def QA_fetch_stock_alpha101half_adv(code, start="all", end=None, collections=DATABASE.stock_alpha101_half):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha101half(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha101half(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_alpha191half_adv(code, start="all", end=None, collections=DATABASE.stock_alpha101_half):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha191half(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha191half(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_half_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.stock_day_half
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_stock_half(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_stock_half_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_half return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_half_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_Stock_day(res_reset_index)

def QA_fetch_stock_week_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.stock_week
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_stock_week(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_stock_week_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_week return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_week_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_Stock_day(res_reset_index)

def QA_fetch_stock_month_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.stock_month
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_stock_month(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_stock_month_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_month return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_month_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_Stock_day(res_reset_index)

def QA_fetch_stock_year_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.stock_year
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_stock_year(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_stock_year_adv parameter code=%s , start=%s, end=%s call QA_fetch_stock_year return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_year_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_Stock_day(res_reset_index)

def QA_fetch_index_week_adv(
        code,
        start,
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.index_week
):
    '''
    :param code: code:  字符串str eg 600085
    :param start:  字符串str 开始日期 eg 2011-01-01
    :param end:  字符串str 结束日期 eg 2011-05-01
    :param if_drop_index: Ture False ， dataframe drop index or not
    :param collections:  mongodb 数据库
    :return:
    '''
    '获取指数日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # 🛠 todo 报告错误 如果开始时间 在 结束时间之后
    # 🛠 todo 如果相等

    res = QA_fetch_index_week(code, start, end, format='pd', collections= collections)
    if res is None:
        print(
            "QA Error QA_fetch_index_week_adv parameter code=%s start=%s end=%s call QA_fetch_index_week return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_set_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_set_index is None:
        #     print("QA Error QA_fetch_index_week_adv set index 'date, code' return None")
        #     return None
        return QA_DataStruct_Index_day(res_set_index)

def QA_fetch_index_month_adv(
        code,
        start,
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.index_month
):
    '''
    :param code: code:  字符串str eg 600085
    :param start:  字符串str 开始日期 eg 2011-01-01
    :param end:  字符串str 结束日期 eg 2011-05-01
    :param if_drop_index: Ture False ， dataframe drop index or not
    :param collections:  mongodb 数据库
    :return:
    '''
    '获取指数日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # 🛠 todo 报告错误 如果开始时间 在 结束时间之后
    # 🛠 todo 如果相等

    res = QA_fetch_index_month(code, start, end, format='pd', collections= collections)
    if res is None:
        print(
            "QA Error QA_fetch_index_month_adv parameter code=%s start=%s end=%s call QA_fetch_index_month return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_set_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_set_index is None:
        #     print("QA Error QA_fetch_index_month_adv set index 'date, code' return None")
        #     return None
        return QA_DataStruct_Index_day(res_set_index)

def QA_fetch_index_year_adv(
        code,
        start,
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.index_year
):
    '''
    :param code: code:  字符串str eg 600085
    :param start:  字符串str 开始日期 eg 2011-01-01
    :param end:  字符串str 结束日期 eg 2011-05-01
    :param if_drop_index: Ture False ， dataframe drop index or not
    :param collections:  mongodb 数据库
    :return:
    '''
    '获取指数日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # 🛠 todo 报告错误 如果开始时间 在 结束时间之后
    # 🛠 todo 如果相等

    res = QA_fetch_index_year(code, start, end, format='pd', collections= collections)
    if res is None:
        print(
            "QA Error QA_fetch_index_year_adv parameter code=%s start=%s end=%s call QA_fetch_index_year return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_set_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_set_index is None:
        #     print("QA Error QA_fetch_index_year_adv set index 'date, code' return None")
        #     return None
        return QA_DataStruct_Index_day(res_set_index)

def QA_fetch_usstock_day_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.usstock_day
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_usstock_day(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_usstock_day_adv parameter code=%s , start=%s, end=%s call QA_fetch_usstock_day return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_day_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_UsStock_day(res_reset_index)

def QA_fetch_stock_alpha191real_adv(code, start="all", end=None):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_alpha101real_adv(code, start="all", end=None):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_alpha101_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_alpha101_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_usstock_xq_day_adv(
        code,
        start='all',
        end=None,
        if_drop_index=True,
        # 🛠 todo collections 参数没有用到， 且数据库是固定的， 这个变量后期去掉
        collections=DATABASE.usstock_day_xq
):
    '''

    :param code:  股票代码
    :param start: 开始日期
    :param end:   结束日期
    :param if_drop_index:
    :param collections: 默认数据库
    :return: 如果股票代码不存 或者开始结束日期不存在 在返回 None ，合法返回 QA_DataStruct_Stock_day 数据
    '''
    '获取股票日线'
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    if start == 'all':
        start = '1990-01-01'
        end = str(datetime.date.today())

    res = QA_fetch_usstock_xq_day(code, start, end, format='pd', collections= collections)
    if res is None:
        # 🛠 todo 报告是代码不合法，还是日期不合法
        print(
            "QA Error QA_fetch_usstock_xq_day_adv parameter code=%s , start=%s, end=%s call QA_fetch_usstock_day return None"
            % (code,
               start,
               end)
        )
        return None
    else:
        res_reset_index = res.set_index(['date', 'code'], drop=if_drop_index)
        # if res_reset_index is None:
        #     print("QA Error QA_fetch_stock_day_adv set index 'datetime, code' return None")
        #     return None
        return QA_DataStruct_UsStock_day(res_reset_index)


def QA_fetch_stock_technical_half_adv(code, start="all", end=None, type='day', collections=DATABASE.stock_technical_index):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_technical_half(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_technical_half(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_usstock_alpha_adv(code, start="all", end=None, collections=DATABASE.usstock_alpha):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_usstock_alpha(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_usstock_alpha(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_usstock_alpha101_adv(code, start="all", end=None, collections=DATABASE.usstock_alpha101):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_usstock_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_usstock_alpha101(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_usstock_technical_index_adv(code, start="all", end=None, type='day', collections=DATABASE.usstock_technical_index):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_usstock_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_usstock_technical_index(code, start, end, type, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_usstock_financial_percent_adv(code, start="all", end=None, collections=DATABASE.usstock_financial_percent):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2008-01-01'
        end = QA_util_today_str()
        data = QA_fetch_usstock_financial_percent(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_usstock_financial_percent(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)

def QA_fetch_stock_base_real_adv(code, start="all", end=None):
    '获取股票财报日历'
    #code= [code] if isinstance(code,str) else code
    end = start if end is None else end
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    if start == 'all' or start == None:
        start = '2005-01-01'
        end = QA_util_today_str()
        data = QA_fetch_stock_base_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
    else:
        data = QA_fetch_stock_base_real(code, start, end, format='pd')
        return QA_DataStruct_Financial(data)
