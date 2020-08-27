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
from  QUANTAXIS.QAUtil import QA_util_today_str
from QUANTAXIS.QASU.main import (QA_SU_save_financialfiles_fromtdx)
from QUANTTOOLS.QAStockETL import (QA_etl_stock_financial, QA_SU_save_fianacialTTM_momgo,QA_SU_save_stock_financial_wy_day,
                                   QA_util_process_stock_financial,QA_etl_stock_financial_wy)
from QUANTTOOLS.QAStockETL.FuncTools.check_data import (check_wy_financial, check_tdx_financial)

if __name__ == '__main__':
    mark_day = QA_util_today_str()
    print("write tdx financial data into sqldatabase")
    while check_tdx_financial(mark_day) is None or check_tdx_financial(mark_day) > 0:
        QA_SU_save_financialfiles_fromtdx()
    QA_etl_stock_financial('all')
    print("done")
    print("write wy financial data into sqldatabase")
    while check_wy_financial(mark_day) is None or check_wy_financial(mark_day) > 0:
        QA_SU_save_stock_financial_wy_day()
    QA_etl_stock_financial_wy('all')
    print("done")
    print("run financial data into sqldatabase")
    QA_util_process_stock_financial()
    QA_SU_save_fianacialTTM_momgo()
    print("done")