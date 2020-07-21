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
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_quant_data_day,
                                   QA_SU_save_index_quant_data_day)
from QUANTTOOLS.QAStockETL.FuncTools.check_data import (check_stock_fianacial,check_stock_finper,
                                                        check_stock_techindex,check_stock_techweek,
                                                        check_stock_alpha101,check_stock_alpha191,
                                                        check_stock_quant,
                                                        check_index_alpha101,check_index_alpha191,
                                                        check_index_techindex,check_index_techweek,
                                                        check_index_quant)
from  QUANTAXIS.QAUtil import QA_util_today_str
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.daily_job import job111
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    while check_stock_fianacial(mark_day) is None or check_stock_fianacial(mark_day)  > 20:
        time.sleep(300)

    while check_stock_finper(mark_day) is None or check_stock_finper(mark_day)  > 20:
        time.sleep(300)

    while check_stock_techindex(mark_day) is None or check_stock_techindex(mark_day)  > 20:
        time.sleep(300)

    while check_stock_techweek(mark_day) is None or check_stock_techweek(mark_day)  > 20:
        time.sleep(300)

    while check_stock_alpha101(mark_day) is None or check_stock_alpha101(mark_day)  > 20:
        time.sleep(300)

    while check_stock_alpha191(mark_day) is None or check_stock_alpha191(mark_day)  > 20:
        time.sleep(300)

    while check_index_alpha101(mark_day) is None or check_index_alpha101(mark_day)  > 10:
        time.sleep(180)

    while check_index_alpha191(mark_day) is None or check_index_alpha191(mark_day)  > 10:
        time.sleep(180)

    while check_index_techindex(mark_day) is None or check_index_techindex(mark_day)  > 10:
        time.sleep(180)

    while check_index_techweek(mark_day) is None or check_index_techweek(mark_day)  > 10:
        time.sleep(180)

    QA_SU_save_stock_quant_data_day(start_date = mark_day, end_date = mark_day)
    QA_SU_save_index_quant_data_day(start_date = mark_day, end_date = mark_day)

    check_stock_quant(mark_day)
    check_index_quant(mark_day)

    job111(mark_day)