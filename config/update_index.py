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
from QUANTTOOLS.QAStockETL import (QA_SU_save_index_min,
                                   QA_SU_save_single_index_min,
                                   QA_SU_save_index_alpha_day,
                                   QA_SU_save_index_technical_hour_day,
                                   QA_SU_save_index_technical_index_day,
                                   QA_SU_save_index_technical_week_day,
                                   QA_SU_save_index_technical_month_day,
                                   QA_SU_save_index_alpha101_day,
                                   QA_SU_save_index_info)
from QUANTAXIS.QASU.main import (QA_SU_save_index_day,QA_SU_save_index_list)
from QUANTTOOLS.QAStockETL.Check import (check_index_day,check_index_60min)
from QUANTTOOLS.QAStockETL.Check import (check_index_alpha101, check_index_alpha191,
                                         check_index_techhour, check_index_techindex, check_index_techweek)
from QUANTTOOLS.QAStockETL import (QA_etl_index_alpha_day,
                                   QA_etl_index_alpha101_day,
                                   QA_etl_index_day,
                                   QA_etl_index_technical_hour,
                                   QA_etl_index_technical_day,
                                   QA_etl_index_technical_week)
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):

        QA_SU_save_index_list('tdx')
        QA_SU_save_index_day('tdx')
        QA_SU_save_index_info()

        res = check_index_day(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_index_day('tdx')
            res = check_index_day(mark_day)

        QA_etl_index_day('day',mark_day)

        res = check_index_techindex(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_index_technical_index_day(start_date = mark_day, end_date = mark_day)
            res = check_index_techindex(mark_day)

        res = check_index_techweek(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_index_technical_week_day(start_date = mark_day, end_date = mark_day)
            res = check_index_techweek(mark_day)

        QA_etl_index_technical_day(mark_day, mark_day)
        QA_etl_index_technical_week(mark_day,  mark_day)

        QA_SU_save_index_technical_month_day(start_date = mark_day, end_date = mark_day)
