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

from QUANTTOOLS.QAStockETL.Check import (check_stock_code, check_stock_neut,
                                         check_stock_finper, check_stock_alpha191, check_stock_vwap,
                                         check_index_techindex, check_index_techhour)
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade,QA_util_get_pre_trade_date,QA_util_get_real_date
from QUANTTOOLS.Market.StockMarket.DailyJob.daily_job import daily_run, index_run, neut_run
from QUANTAXIS.QASU.main import (QA_SU_save_stock_list,QA_SU_save_stock_info_tushare)
from QUANTTOOLS.QAStockETL import QA_SU_save_stock_aklist
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        check_day = QA_util_get_pre_trade_date(mark_day,1)
    else:
        check_day = QA_util_get_real_date(mark_day)

    QA_SU_save_stock_aklist()
    res = check_stock_code()
    if len(res) > 0:
        QA_SU_save_stock_list('tdx')
        #QA_SU_save_stock_info_tushare()
        #QA_SU_save_stock_industryinfo()
        res = check_stock_code()

    res = check_stock_alpha191(mark_day)
    while res is None or len(res[1]) > 20:
        time.sleep(180)
        res = check_stock_alpha191(mark_day)

    res = check_stock_finper(mark_day)
    while res is None or len(res[1]) > 100:
        time.sleep(180)
        res = check_stock_finper(mark_day)

    #res = check_stock_vwap(mark_day)
    #while res is None or len(res[1]) > 50:
    #    time.sleep(180)
    #    res = check_stock_vwap(mark_day)

    daily_run(mark_day)

    res = check_index_techindex(mark_day)
    while res is None or (len(res[0]) + len(res[1])) > 20:
        time.sleep(180)
        res = check_index_techindex(mark_day)

    res = check_index_techhour(mark_day)
    while res is None or (len(res[0]) + len(res[1])) > 20:
        time.sleep(180)
        res = check_index_techhour(mark_day)
    index_run(mark_day)