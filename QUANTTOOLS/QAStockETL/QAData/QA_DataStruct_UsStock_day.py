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
"""
定义一些可以扩展的数据结构

方便序列化/相互转换

"""

import datetime
import itertools
import os
import platform
import statistics
import sys
import time
import webbrowser
from copy import copy
from functools import lru_cache, partial, reduce

import numpy as np
import pandas as pd
try:
    from pyecharts import Kline
except:
    from pyecharts.charts import Kline

from QUANTAXIS.QAData.base_datastruct import _quotation_base
from QUANTTOOLS.QAStockETL.QAData.data_fq import QA_data_usstock_to_fq
from QUANTAXIS.QAData.data_resample import (
    QA_data_tick_resample,
    QA_data_day_resample,
    QA_data_futureday_resample,
    QA_data_min_resample,
    QA_data_futuremin_resample,
    QA_data_cryptocurrency_min_resample
)
from QUANTAXIS.QAIndicator import EMA, HHV, LLV, SMA
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_log_info,
    QA_util_random_with_topic,
    QA_util_to_json_from_pandas,
    QA_util_date_valid,
    QA_util_code_tolist,
    QA_util_to_pandas_from_json,
    trade_date_sse,
    QA_util_date_stamp
)
from QUANTAXIS.QAUtil.QADate import QA_util_to_datetime
from QUANTAXIS.QAUtil.QAParameter import FREQUENCE, MARKET_TYPE


def _QA_fetch_usstock_adj(
        code,
        start,
        end,
        format='pd',
        collections=DATABASE.usstock_adj
):
    """获取股票复权系数 ADJ

    """

    start = str(start)[0:10]
    end = str(end)[0:10]
    #code= [code] if isinstance(code,str) else code

    # code checking
    #code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        cursor = collections.find(
            {
                'code': {
                    '$in': code
                },
                "date_stamp": {
                    "$lte": QA_util_date_stamp(end),
                    "$gte": QA_util_date_stamp(start)
                }
            },
            {"_id": 0},
            batch_size=10000
        )
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        res.date = pd.to_datetime(res.date)
        return res.set_index('date', drop=False)

class QA_DataStruct_UsStock_day(_quotation_base):
    '''

        股票日线数据
    '''

    def __init__(self, init_data_by_df, dtype='stock_day', if_fq='bfq'):
        '''
        # 🛠 todo dtype=stock_day 和 QA_DataStruct_Stock_day 类的名字是对应的 不变的不需要指定 ，容易出错，建议改成常量 ❌
        :param init_data_by_df:  DataFrame 类型的数据，包含了数据，用来初始化这个类
        :param dtype:  stock_day 🛠 todo 改成常量
        :param if_fq:  是否复权
        '''
        super().__init__(init_data_by_df, dtype, if_fq)

        if isinstance(init_data_by_df, pd.DataFrame) == False:
            print("QAError init_data_by_df is not kind of DataFrame type !")

    # 抽象类继承

    def choose_db(self):
        self.mongo_coll = DATABASE.usstock_day

    def __repr__(self):
        return '< QA_DataStruct_UsStock_day with {} securities >'.format(
            len(self.code)
        )

    __str__ = __repr__

    # 前复权
    def to_qfq(self):
        if self.if_fq is 'bfq':
            if len(self.code) < 1:
                self.if_fq = 'qfq'
                return self
            # elif len(self.code) < 20:
            #     return self.new(pd.concat(list(map(
            #         lambda x: QA_data_usstock_to_fq(self.data[self.data['code'] == x]), self.code))), self.type, 'qfq')
            else:
                try:
                    date = self.date
                    adj = _QA_fetch_usstock_adj(
                        self.code.to_list(),
                        str(date[0])[0:10],
                        str(date[-1])[0:10]
                    ).set_index(['date',
                                 'code'])
                    data = self.data.join(adj)
                    for col in ['open', 'high', 'low', 'close']:
                        data[col] = data[col] * data['adj'] + data['adjust']
                    # data['volume'] = data['volume'] / \
                    #     data['adj'] if 'volume' in data.columns else data['vol']/data['adj']

                    data['volume'] = data['volume']  if 'volume' in data.columns else data['vol']
                    try:
                        data['high_limit'] = data['high_limit'] * data['adj'] + data['adjust']
                        data['low_limit'] = data['high_limit'] * data['adj'] + data['adjust']
                    except:
                        pass
                    return self.new(data, self.type, 'qfq')
                except Exception as e:
                    print(e)
                    print('use old model qfq')
                    return self.new(
                        self.groupby(level=1).apply(QA_data_usstock_to_fq,
                                                    'qfq'),
                        self.type,
                        'qfq'
                    )
        else:
            QA_util_log_info(
                'none support type for qfq Current type is: %s' % self.if_fq
            )
            return self

    # 后复权
    def to_hfq(self):
        if self.if_fq is 'bfq':
            if len(self.code) < 1:
                self.if_fq = 'hfq'
                return self
            else:
                return self.new(
                    self.groupby(level=1).apply(QA_data_usstock_to_fq,
                                                'hfq'),
                    self.type,
                    'hfq'
                )
                # return self.new(pd.concat(list(map(lambda x: QA_data_stock_to_fq(
                #     self.data[self.data['code'] == x], 'hfq'), self.code))), self.type, 'hfq')
        else:
            QA_util_log_info(
                'none support type for qfq Current type is: %s' % self.if_fq
            )
            return self

    @property
    @lru_cache()
    def high_limit(self):
        '涨停价'
        return self.groupby(
            level=1
        ).close.apply(lambda x: round((x.shift(1) + 0.0002) * 1.1,
                                      2)).sort_index()

    @property
    @lru_cache()
    def low_limit(self):
        '跌停价'
        return self.groupby(
            level=1
        ).close.apply(lambda x: round((x.shift(1) + 0.0002) * 0.9,
                                      2)).sort_index()

    @property
    @lru_cache()
    def next_day_low_limit(self):
        "明日跌停价"
        return self.groupby(
            level=1
        ).close.apply(lambda x: round((x + 0.0002) * 0.9,
                                      2)).sort_index()

    @property
    @lru_cache()
    def next_day_high_limit(self):
        "明日涨停价"
        return self.groupby(
            level=1
        ).close.apply(lambda x: round((x + 0.0002) * 1.1,
                                      2)).sort_index()

    @property
    def preclose(self):
        try:
            return self.data.preclose
        except:
            return None

    pre_close = preclose

    @property
    def price_chg(self):
        try:
            return (self.close - self.preclose) / self.preclose
        except:
            return None

    @property
    @lru_cache()
    def week(self):
        return self.resample('w')

    @property
    @lru_cache()
    def month(self):
        return self.resample('M')

    @property
    @lru_cache()
    def quarter(self):
        return self.resample('Q')

    # @property
    # @lru_cache()
    # def semiannual(self):
    #     return self.resample('SA')

    @property
    @lru_cache()
    def year(self):
        return self.resample('Y')

    def resample(self, level):
        try:
            return self.add_func(QA_data_day_resample, level).sort_index()
        except Exception as e:
            print('QA ERROR : FAIL TO RESAMPLE {}'.format(e))
            return None

class QA_DataStruct_UsStock_min(_quotation_base):

    def __init__(self, DataFrame, dtype='stock_min', if_fq='bfq'):
        super().__init__(DataFrame, dtype, if_fq)

        try:
            if 'preclose' in DataFrame.columns:
                self.data = DataFrame.loc[:,
                            [
                                'open',
                                'high',
                                'low',
                                'close',
                                'volume',
                                'amount',
                                'preclose',
                                'type'
                            ]]
            else:
                self.data = DataFrame.loc[:,
                            [
                                'open',
                                'high',
                                'low',
                                'close',
                                'volume',
                                'amount',
                                'type'
                            ]]
        except Exception as e:
            raise e

        self.type = dtype
        self.if_fq = if_fq

        self.data = self.data.sort_index()

    # 抽象类继承
    def choose_db(self):
        self.mongo_coll = DATABASE.stock_min

    def __repr__(self):
        return '< QA_DataStruct_Stock_Min with {} securities >'.format(
            len(self.code)
        )

    __str__ = __repr__

    def to_qfq(self):
        if self.if_fq is 'bfq':
            if len(self.code) < 1:
                self.if_fq = 'qfq'
                return self
            # elif len(self.code) < 20:
            #     data = QA_DataStruct_Stock_min(pd.concat(list(map(lambda x: QA_data_stock_to_fq(
            #         self.data[self.data['code'] == x]), self.code))).set_index(['datetime', 'code'], drop=False))
            #     data.if_fq = 'qfq'
            #     return data
            else:
                try:
                    date = self.date
                    adj = _QA_fetch_usstock_adj(
                        self.code.to_list(),
                        str(date[0])[0:10],
                        str(date[-1])[0:10]
                    ).set_index(['date',
                                 'code'])
                    u = self.data.reset_index()
                    u = u.assign(date=u.datetime.apply(lambda x: x.date()))
                    u = u.set_index(['date', 'code'], drop=False)

                    data = u.join(adj).set_index(['datetime', 'code'])

                    for col in ['open', 'high', 'low', 'close']:
                        data[col] = data[col] * data['adj'] + data['adjust']
                    # data['volume'] = data['volume'] / \
                    #     data['adj']
                    #data['volume'] = data['volume']  if 'volume' in data.columns else data['vol']
                    try:
                        data['high_limit'] = data['high_limit'] * data['adj'] + data['adjust']
                        data['low_limit'] = data['high_limit'] * data['adj'] + data['adjust']
                    except:
                        pass
                    return self.new(data, self.type, 'qfq')
                except Exception as e:
                    print(e)
                    print('use old model qfq')
                    return self.new(
                        self.groupby(level=1).apply(QA_data_usstock_to_fq,
                                                    'qfq'),
                        self.type,
                        'qfq'
                    )

        else:
            QA_util_log_info(
                'none support type for qfq Current type is:%s' % self.if_fq
            )
            return self

    def to_hfq(self):
        if self.if_fq is 'bfq':
            if len(self.code) < 1:
                self.if_fq = 'hfq'
                return self
            else:
                return self.new(
                    self.groupby(level=1).apply(QA_data_usstock_to_fq,
                                                'hfq'),
                    self.type,
                    'hfq'
                )
                # data = QA_DataStruct_Stock_min(pd.concat(list(map(lambda x: QA_data_stock_to_fq(
                #     self.data[self.data['code'] == x], 'hfq'), self.code))).set_index(['datetime', 'code'], drop=False))
                # data.if_fq = 'hfq'
                # return data
        else:
            QA_util_log_info(
                'none support type for qfq Current type is:%s' % self.if_fq
            )
            return self

    # @property
    # def high_limit(self):
    #     '涨停价'
    #     return self.data.high_limit

    # @property
    # def low_limit(self):
    #     '跌停价'
    #     return self.data.low_limit

    def resample(self, level):
        try:
            return self.add_funcx(QA_data_min_resample, level).sort_index()
        except Exception as e:
            print('QA ERROR : FAIL TO RESAMPLE {}'.format(e))
            return None

    @property
    @lru_cache()
    def min5(self):
        return self.resample('5min')

    @property
    @lru_cache()
    def min15(self):
        return self.resample('15min')

    @property
    @lru_cache()
    def min30(self):
        return self.resample('30min')

    @property
    @lru_cache()
    def min60(self):
        return self.resample('60min')