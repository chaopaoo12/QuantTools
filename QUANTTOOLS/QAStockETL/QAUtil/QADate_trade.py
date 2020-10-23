# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2020 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import pandas as pd
from QUANTAXIS.QAUtil.QADate_trade import trade_date_sse

from QUANTAXIS.QAUtil.QAParameter import (
    MARKET_TYPE,
    FREQUENCE
)

def QA_util_format_date2str(cursor_date):
    """
    explanation:
        对输入日期进行格式化处理，返回格式为 "%Y-%m-%d" 格式字符串
        支持格式包括:
        1. str: "%Y%m%d" "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S",
                "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H%M%S"
        2. datetime.datetime
        3. pd.Timestamp
        4. int -> 自动在右边加 0 然后转换，譬如 '20190302093' --> "2019-03-02"
    params:
        * cursor_date->
            含义: 输入日期
            类型: str
            参数支持: []
    """
    if isinstance(cursor_date, datetime.datetime):
        cursor_date = str(cursor_date)[:10]
    elif isinstance(cursor_date, str):
        try:
            cursor_date = str(pd.Timestamp(cursor_date))[:10]
        except:
            raise ValueError('请输入正确的日期格式, 建议 "%Y-%m-%d"')
    elif isinstance(cursor_date, int):
        cursor_date = str(pd.Timestamp("{:<014d}".format(cursor_date)))[:10]
    else:
        raise ValueError('请输入正确的日期格式，建议 "%Y-%m-%d"')
    return cursor_date


def QA_util_get_next_period(datetime, frequence='1min'):
    '''
    得到给定频率的下一个周期起始时间
    :param datetime: 类型 datetime eg: 2018-11-11 13:01:01
    :param frequence: 类型 str eg: '30min'
    :return: datetime eg: 2018-11-11 13:31:00
    '''
    freq = {
        FREQUENCE.YEAR: 'Y',
        FREQUENCE.QUARTER: 'Q',
        FREQUENCE.MONTH: 'M',
        FREQUENCE.WEEK: 'W',
        FREQUENCE.DAY: 'D',
        FREQUENCE.SIXTY_MIN: '60T',
        FREQUENCE.THIRTY_MIN: '30T',
        FREQUENCE.FIFTEEN_MIN: '15T',
        FREQUENCE.FIVE_MIN: '5T',
        FREQUENCE.ONE_MIN: 'T'
    }
    return (pd.Period(datetime, freq=freq[frequence]) + 1).to_timestamp()


def QA_util_get_next_trade_date(cursor_date, n=1, trade_date_dict = trade_date_sse):
    """
    explanation:
        根据输入日期得到下 n 个交易日 (不包含当前交易日)

    params:
        * cursor_date->
            含义: 输入日期
            类型: str
            参数支持: []
        * n->
            含义: 步长,默认为1
            类型: int
            参数支持: [int]
    """

    cursor_date = QA_util_format_date2str(cursor_date)
    if cursor_date in trade_date_dict:
        # 如果指定日期为交易日
        return QA_util_date_gap(cursor_date, n, "gt")
    real_pre_trade_date = QA_util_get_real_date(cursor_date)
    return QA_util_date_gap(real_pre_trade_date, n, "gt")


def QA_util_get_pre_trade_date(cursor_date, n=1, trade_date_dict = trade_date_sse):
    """
    explanation:
        得到前 n 个交易日 (不包含当前交易日)

    params:
        * cursor_date->
            含义: 输入日期
            类型: str
            参数支持: []
        * n->
            含义: 步长,默认为1
            类型: int
            参数支持: [int]
    """

    cursor_date = QA_util_format_date2str(cursor_date)
    if cursor_date in trade_date_dict:
        return QA_util_date_gap(cursor_date, n, "lt", trade_date_dict)
    real_aft_trade_date = QA_util_get_real_date(cursor_date)
    return QA_util_date_gap(real_aft_trade_date, n, "lt", trade_date_dict)


def QA_util_if_trade(day, trade_date_dict = trade_date_sse):
    """
    得到前 n 个交易日 (不包含当前交易日)
    '日期是否交易'
    查询上面的 交易日 列表
    :param day: 类型 str eg: 2018-11-11
    :return: Boolean 类型
    """
    if day in trade_date_dict:
        return True
    else:
        return False

def QA_util_get_next_day(date, n=1, trade_date_dict =trade_date_sse):
    """
    explanation:
        得到下一个(n)交易日

    params:
        * date->
            含义: 日期
            类型: str
            参数支持: []
        * n->
            含义: 步长
            类型: int
            参数支持: [int]
    """
    date = str(date)[0:10]
    return QA_util_date_gap(date, n, 'gt', trade_date_dict)


def QA_util_get_last_day(date, n=1, trade_date_dict =trade_date_sse):
    """
    explanation:
       得到上一个(n)交易日

    params:
        * date->
            含义: 日期
            类型: str
            参数支持: []
        * n->
            含义: 步长
            类型: int
            参数支持: [int]
    """
    date = str(date)[0:10]
    return QA_util_date_gap(date, n, 'lt', trade_date_dict)


def QA_util_get_last_datetime(datetime, day=1, trade_date_dict =trade_date_sse):
    """
    explanation:
        获取几天前交易日的时间

    params:
        * datetime->
            含义: 指定时间
            类型: datetime
            参数支持: []
        * day->
            含义: 指定时间
            类型: int
            参数支持: []
    """

    date = str(datetime)[0:10]
    return '{} {}'.format(QA_util_date_gap(date, day, 'lt'), str(datetime)[11:], trade_date_dict)


def QA_util_get_next_datetime(datetime, day=1, trade_date_dict =trade_date_sse):
    date = str(datetime)[0:10]
    return '{} {}'.format(QA_util_date_gap(date, day, 'gt'), str(datetime)[11:], trade_date_dict)


def QA_util_get_real_date(date, trade_list=trade_date_sse, towards=-1):
    """
    explanation:
        获取真实的交易日期

    params:
        * date->
            含义: 日期
            类型: date
            参数支持: []
        * trade_list->
            含义: 交易列表
            类型: List
            参数支持: []
        * towards->
            含义: 方向， 1 -> 向前, -1 -> 向后
            类型: int
            参数支持: [1， -1]
    """
    date = str(date)[0:10]
    if towards == 1:
        while date not in trade_list:
            date = str(
                datetime.datetime.strptime(str(date)[0:10],
                                           '%Y-%m-%d') +
                datetime.timedelta(days=1)
            )[0:10]
        else:
            return str(date)[0:10]
    elif towards == -1:
        while date not in trade_list:
            date = str(
                datetime.datetime.strptime(str(date)[0:10],
                                           '%Y-%m-%d') -
                datetime.timedelta(days=1)
            )[0:10]
        else:
            return str(date)[0:10]


def QA_util_get_real_datelist(start, end, trade_date_dict =trade_date_sse):
    """
    explanation:
        取数据的真实区间，当start end中间没有交易日时返回None, None,
        同时返回的时候用 start,end=QA_util_get_real_datelist

    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
    """
    real_start = QA_util_get_real_date(start, trade_date_dict, 1)
    real_end = QA_util_get_real_date(end, trade_date_dict, -1)
    if trade_date_dict.index(real_start) > trade_date_dict.index(real_end):
        return None, None
    else:
        return (real_start, real_end)


def QA_util_get_trade_range(start, end, trade_date_dict =trade_date_sse):
    """
    explanation:
       给出交易具体时间

    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
    """
    start, end = QA_util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_dict[trade_date_dict
                                  .index(start):trade_date_dict.index(end) + 1:1]
    else:
        return None


def QA_util_get_trade_gap(start, end, trade_date_dict =trade_date_sse):
    """
    explanation:
        返回start_day到end_day中间有多少个交易天 算首尾

    params:
        * start->
            含义: 开始日期
            类型: date
            参数支持: []
        * end->
            含义: 截至日期
            类型: date
            参数支持: []
   """
    start, end = QA_util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_dict.index(end) + 1 - trade_date_dict.index(start)
    else:
        return 0


def QA_util_date_gap(date, gap, methods, trade_date_dict =trade_date_sse):
    """
    explanation:
        返回start_day到end_day中间有多少个交易天 算首尾

    params:
        * date->
            含义: 字符串起始日
            类型: str
            参数支持: []
        * gap->
            含义: 间隔多数个交易日
            类型: int
            参数支持: [int]
        * methods->
            含义: 方向
            类型: str
            参数支持: ["gt->大于", "gte->大于等于","小于->lt", "小于等于->lte", "等于->==="]
    """
    try:
        if methods in ['>', 'gt']:
            return trade_date_dict[trade_date_dict.index(date) + gap]
        elif methods in ['>=', 'gte']:
            return trade_date_dict[trade_date_dict.index(date) + gap - 1]
        elif methods in ['<', 'lt']:
            return trade_date_dict[trade_date_dict.index(date) - gap]
        elif methods in ['<=', 'lte']:
            return trade_date_dict[trade_date_dict.index(date) - gap + 1]
        elif methods in ['==', '=', 'eq']:
            return date

    except:
        return 'wrong date'


def QA_util_get_trade_datetime(dt=datetime.datetime.now(), trade_date_dict=trade_date_sse):
    """
    explanation:
        获取交易的真实日期

    params:
        * dt->
            含义: 时间
            类型: datetime
            参数支持: []
    """

    #dt= datetime.datetime.now()

    if QA_util_if_trade(str(dt.date())) and dt.time() < datetime.time(15, 0, 0):
        return str(dt.date())
    else:
        return QA_util_get_real_date(str(dt.date()), trade_date_dict, 1)


def QA_util_get_order_datetime(dt, trade_date_dict=trade_date_sse):
    """
    explanation:
        获取委托的真实日期

    params:
        * dt->
            含义: 委托的时间
            类型: datetime
            参数支持: []

    """

    #dt= datetime.datetime.now()
    dt = datetime.datetime.strptime(str(dt)[0:19], '%Y-%m-%d %H:%M:%S')

    if QA_util_if_trade(str(dt.date())) and dt.time() < datetime.time(15, 0, 0):
        return str(dt)
    else:
        # print('before')
        # print(QA_util_date_gap(str(dt.date()),1,'lt'))
        return '{} {}'.format(
            QA_util_date_gap(str(dt.date()),
                             1,
                             'lt', trade_date_dict),
            dt.time()
        )


def QA_util_future_to_tradedatetime(real_datetime):
    """
    explanation:
        输入是真实交易时间,返回按期货交易所规定的时间* 适用于tb/文华/博弈的转换

    params:
        * real_datetime->
            含义: 真实交易时间
            类型: datetime
            参数支持: []
    """
    if len(str(real_datetime)) >= 19:
        dt = datetime.datetime.strptime(
            str(real_datetime)[0:19],
            '%Y-%m-%d %H:%M:%S'
        )
        return dt if dt.time(
        ) < datetime.time(21,
                          0) else QA_util_get_next_datetime(dt,
                                                            1)
    elif len(str(real_datetime)) == 16:
        dt = datetime.datetime.strptime(
            str(real_datetime)[0:16],
            '%Y-%m-%d %H:%M'
        )
        return dt if dt.time(
        ) < datetime.time(21,
                          0) else QA_util_get_next_datetime(dt,
                                                            1)


def QA_util_future_to_realdatetime(trade_datetime):
    """
    explanation:
       输入是交易所规定的时间,返回真实时间*适用于通达信的时间转换

    params:
        * trade_datetime->
            含义: 真实交易时间
            类型: datetime
            参数支持: []
    """
    if len(str(trade_datetime)) == 19:
        dt = datetime.datetime.strptime(
            str(trade_datetime)[0:19],
            '%Y-%m-%d %H:%M:%S'
        )
        return dt if dt.time(
        ) < datetime.time(21,
                          0) else QA_util_get_last_datetime(dt,
                                                            1)
    elif len(str(trade_datetime)) == 16:
        dt = datetime.datetime.strptime(
            str(trade_datetime)[0:16],
            '%Y-%m-%d %H:%M'
        )
        return dt if dt.time(
        ) < datetime.time(21,
                          0) else QA_util_get_last_datetime(dt,
                                                            1)
