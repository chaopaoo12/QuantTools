#!/usr/local/bin/python

#coding :utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
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


"""对应于save x
"""

from QUANTTOOLS.QAStockETL.FuncTools.check_data import (
                                                        check_stock_quant,
                                                        check_index_quant)
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_real_date
from QUANTTOOLS.StockMarket.StockStrategySecond.daily_job import job111
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()
    ckeck_day = QA_util_get_real_date(mark_day)

    while check_stock_quant(ckeck_day) is None or check_stock_quant(ckeck_day)  > 10:
        time.sleep(180)

    while check_index_quant(ckeck_day) is None or check_index_quant(ckeck_day)  > 10:
        time.sleep(180)

    job111(mark_day)