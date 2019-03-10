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

from QAStockETL.QASU import crawl_jrj_financial_reportdate as save_financial_calendar
from QAStockETL.QASU import crawl_jrj_stock_divyield as save_stock_divyield
from QAStockETL.QASU import save_financial_TTM as save_financial_TTM
from QAStockETL.QASU import save_stock_financial as save_stock_financial
from QUANTAXIS.QAUtil import QA_util_today_str

def QA_SU_save_report_calendar_day():
    return save_financial_calendar.QA_SU_save_report_calendar_day()


def QA_SU_save_report_calendar_his():
    return save_financial_calendar.QA_SU_save_report_calendar_his()


def QA_SU_save_stock_divyield_day():
    return save_stock_divyield.QA_SU_save_stock_divyield_day()


def QA_SU_save_stock_divyield_his():
    return save_stock_divyield.QA_SU_save_stock_divyield_his()

def QA_SU_save_fianacialTTM_momgo():
    return save_financial_TTM.QA_SU_save_fianacialTTM_momgo()

def QA_SU_save_stock_fianacial_momgo(start_date=None,end_date=None):
    return save_stock_financial.QA_SU_save_stock_fianacial_momgo(start_date,end_date)

def QA_SU_save_stock_fianacial_momgo_his(start_date=None,end_date=QA_util_today_str()):
    return save_stock_financial.QA_SU_save_stock_fianacial_momgo(start_date,end_date)