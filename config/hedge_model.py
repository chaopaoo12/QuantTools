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

from QUANTTOOLS.QAStockETL.Check import (check_stock_quant,check_stock_day,check_stock_adj,check_stock_alpha191real,check_stock_alpha101real)
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_real_date,QA_util_get_pre_trade_date
from QUANTTOOLS.Market.StockMarket.DailyJob.train_job import hedge_train
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()
    ckeck_day = QA_util_get_real_date(mark_day)
    ckeck_day1 = QA_util_get_pre_trade_date(mark_day,1)

    check = check_stock_day(ckeck_day)
    while check is None or (len(check[0]) + len(check[1])) > 10:
        time.sleep(180)
        check = check_stock_day(ckeck_day)

    check = check_stock_adj(ckeck_day)
    while check is None or (len(check[0]) + len(check[1])) > 10:
        time.sleep(180)
        check = check_stock_adj(ckeck_day)

    check = check_stock_quant(ckeck_day1)
    while check is None or (len(check[0]) + len(check[1])) > 10:
        time.sleep(180)
        check = check_stock_quant(ckeck_day1)

    check = check_stock_alpha191real(ckeck_day)
    while check is None or (len(check[0]) + len(check[1])) > 10:
        time.sleep(180)
        check = check_stock_alpha191real(ckeck_day)

    check = check_stock_alpha101real(ckeck_day)
    while check is None or (len(check[0]) + len(check[1])) > 10:
        time.sleep(180)
        check = check_stock_alpha101real(ckeck_day)

    hedge_train(mark_day)