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
from QUANTAXIS.QASU.main import (QA_SU_save_etf_day, QA_SU_save_etf_min,
                                 QA_SU_save_financialfiles,
                                 QA_SU_save_index_day, QA_SU_save_index_min,
                                 QA_SU_save_stock_block, QA_SU_save_stock_day,
                                 QA_SU_save_stock_info,
                                 QA_SU_save_stock_info_tushare,
                                 QA_SU_save_stock_list, QA_SU_save_stock_min,
                                 QA_SU_save_stock_xdxr,QA_SU_save_index_list)
from QUANTTOOLS.QAStockETL import (QA_etl_stock_list, QA_etl_stock_info,
                                   QA_etl_stock_xdxr, QA_etl_stock_day,
                                   QA_etl_stock_financial, QA_etl_stock_calendar,
                                   QA_etl_stock_block, QA_etl_stock_divyield,
                                   QA_etl_process_financial_day,QA_SU_save_stock_alpha_day,
                                   QA_SU_save_stock_technical_index_day,
                                   QA_SU_save_stock_fianacial_percent_day,
                                   QA_etl_stock_alpha_day,QA_util_process_stock_financial,
                                   QA_etl_stock_technical_day,QA_SU_save_stock_quant_data_day,
                                   QA_SU_save_stock_fianacial_momgo,QA_SU_save_fianacialTTM_momgo,
                                   QA_SU_save_stock_technical_week_day,QA_SU_save_stock_technical_month_day)

print("download day data ")
QA_SU_save_stock_day('tdx')
QA_SU_save_stock_xdxr('tdx')
QA_SU_save_stock_list('tdx')
QA_SU_save_index_list('tdx')
QA_SU_save_stock_block('tdx')
QA_SU_save_stock_info('tdx')
QA_SU_save_stock_info_tushare()
QA_SU_save_index_day('tdx')
QA_SU_save_stock_alpha_day()
QA_SU_save_stock_technical_index_day()
QA_SU_save_stock_technical_week_day()
print("done")
print("write data into sqldatabase")
QA_etl_stock_list()
QA_etl_stock_info()
QA_etl_stock_xdxr(type == "all")
QA_etl_stock_day()
QA_etl_stock_block()
QA_SU_save_index_day('tdx')
#QA_etl_stock_alpha_day("day")
#QA_etl_stock_technical_day("day")
print("done")
print("run financial data into sqldatabase")
QA_util_process_stock_financial()
QA_SU_save_fianacialTTM_momgo()
print("done")
print("processing quant data in sqldatabase")
QA_etl_process_financial_day('day')
print("done")
print("write quant data into mongodb")
QA_SU_save_stock_fianacial_momgo()
print("save quant indicator")
QA_SU_save_stock_fianacial_percent_day()
QA_SU_save_stock_quant_data_day()
print("done")
QA_SU_save_stock_technical_month_day()