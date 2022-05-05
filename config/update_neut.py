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
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_neutral_day,QA_SU_save_stock_quant_data_day)
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import (check_stock_quant,check_stock_code,check_stock_neut,
                                         check_stock_finper,check_stock_alpha191,check_stock_techhour,
                                         check_stock_techweek,check_stock_techhour,
                                         check_stock_techindex,check_index_techweek,
                                         check_index_techindex,check_index_techhour)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()
    if QA_util_if_trade(mark_day):

        res = check_stock_alpha191(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res = check_stock_alpha191(mark_day)

        res = check_stock_finper(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res = check_stock_finper(mark_day)

        res = check_stock_techindex(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res = check_stock_techindex(mark_day)

        res = check_stock_techhour(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res =check_stock_techhour(mark_day)

        res = check_stock_quant(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res = check_stock_quant(mark_day)

        res = check_stock_neut(mark_day)
        while res is None or len(res[1]) > 500:
            QA_SU_save_stock_neutral_day(start_date=mark_day,end_date=mark_day)
            res = check_stock_neut(mark_day)