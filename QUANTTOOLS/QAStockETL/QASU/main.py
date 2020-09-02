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

from QUANTTOOLS.QAStockETL.QASU import crawl_jrj_financial_reportdate as save_financial_calendar, \
    crawl_jrj_stock_divyield as save_stock_divyield, save_stock_financial as save_stock_financial, \
    save_financial_TTM as save_financial_TTM, crawl_ths_financial_report as save_stock_financial_ths,\
    save_stock_alpha as save_stock_alpha, save_financialfiles as save_financialfiles,\
    crawl_sina_financial_report as save_stock_financial_sina, crawl_sina_shares_change as save_stock_shares_sina,\
    crawl_wy_financial_report as save_stock_financial_wy, save_stock_technical_index as save_stock_technical_index,\
    crawl_interest_rate as crawl_interest_rate, save_stock_finper as save_stock_finper, save_stock_quant as save_stock_quant,\
    crawl_sina_usstock as crawl_sina_hkstock, save_tdx as save_tdx
from QUANTAXIS.QAUtil import QA_util_today_str

def QA_SU_save_report_calendar_day():
    save_financial_calendar.QA_SU_save_report_calendar_day()

def QA_SU_save_report_calendar_his():
    save_financial_calendar.QA_SU_save_report_calendar_his()

def QA_SU_save_stock_divyield_day():
    save_stock_divyield.QA_SU_save_stock_divyield_day()

def QA_SU_save_stock_divyield_his():
    save_stock_divyield.QA_SU_save_stock_divyield_his()

def QA_SU_save_fianacialTTM_momgo():
    save_financial_TTM.QA_SU_save_fianacialTTM_momgo()

def QA_SU_save_stock_fianacial_momgo(start_date=None,end_date=None):
    save_stock_financial.QA_SU_save_stock_fianacial_momgo(start_date, end_date)

def QA_SU_save_stock_fianacial_momgo_his(start_date=None,end_date=QA_util_today_str()):
    save_stock_financial.QA_SU_save_stock_fianacial_momgo_his(start_date, end_date)

def QA_SU_save_stock_financial_ths_day():
    save_stock_financial_ths.QA_SU_save_financial_report_day()

def QA_SU_save_stock_financial_ths_his():
    save_stock_financial_ths.QA_SU_save_financial_report_his()

def QA_SU_save_stock_financial_wy_day(code=None):
    save_stock_financial_wy.QA_SU_save_financial_report_day(code=code)

def QA_SU_save_stock_financial_wy_his():
    save_stock_financial_wy.QA_SU_save_financial_report_his()

def QA_SU_save_stock_financial_sina_day():
    save_stock_financial_sina.QA_SU_save_financial_report_day()

def QA_SU_save_stock_financial_sina_his():
    save_stock_financial_sina.QA_SU_save_financial_report_his()

def QA_SU_save_stock_shares_sina_day():
    save_stock_shares_sina.QA_SU_save_stock_shares_day()

def QA_SU_save_stock_shares_sina_his():
    save_stock_shares_sina.QA_SU_save_stock_shares_his()

def QA_SU_save_stock_technical_index_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_index_day(start_date=start_date,end_date=end_date)

def QA_SU_save_stock_technical_index_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_index_his(start_date=start_date,end_date=end_date)

def QA_SU_save_stock_alpha_day(code = None, start_date = QA_util_today_str(), end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_stock_alpha_day(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_stock_alpha_his(code = None, start_date = None, end_date = None):
    save_stock_alpha.QA_SU_save_stock_alpha_his(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_financialfiles():
    save_financialfiles.QA_SU_save_financial_files()

def QA_SU_save_interest_rate():
    crawl_interest_rate.QA_SU_save_interest_rate()

def QA_SU_save_stock_fianacial_percent_day(code = None, start_date = None, end_date = None):
    save_stock_finper.QA_SU_save_stock_fianacial_percent(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_stock_fianacial_percent_his(code = None, start_date = '2008-01-01', end_date = None):
    save_stock_finper.QA_SU_save_stock_fianacial_percent_his(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_stock_quant_data_day(code = None, start_date = None, end_date = None):
    save_stock_quant.QA_SU_save_stock_quant_day(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_stock_quant_data_his(code = None, start_date = '2008-01-01', end_date = QA_util_today_str()):
    save_stock_quant.QA_SU_save_stock_quant_his(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_stock_technical_week_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_week_day(start_date=start_date,end_date=end_date)

def QA_SU_save_stock_technical_week_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_week_his(start_date=start_date,end_date=end_date)

def QA_SU_save_stock_technical_month_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_month_day(start_date=start_date,end_date=end_date)

def QA_SU_save_stock_technical_month_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_stock_technical_month_his(start_date=start_date,end_date=end_date)

def QA_SU_save_usstock_list_day():
    crawl_sina_hkstock.QA_SU_save_usstock_list_day()

def QA_SU_save_index_alpha_day(code = None, start_date = QA_util_today_str(), end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_index_alpha_day(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_index_alpha_his(code = None, start_date = None, end_date = None):
    save_stock_alpha.QA_SU_save_index_alpha_his(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_index_technical_index_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_index_day(start_date=start_date,end_date=end_date)

def QA_SU_save_index_technical_index_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_index_his(start_date=start_date,end_date=end_date)

def QA_SU_save_index_technical_week_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_week_day(start_date=start_date,end_date=end_date)

def QA_SU_save_index_technical_week_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_week_his(start_date=start_date,end_date=end_date)

def QA_SU_save_index_technical_month_day(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_month_day(start_date=start_date,end_date=end_date)

def QA_SU_save_index_technical_month_his(start_date=None,end_date=None):
    save_stock_technical_index.QA_SU_save_index_technical_month_his(start_date=start_date,end_date=end_date)

def QA_SU_save_index_quant_data_day(code = None, start_date = None, end_date = None):
    save_stock_quant.QA_SU_save_index_quant_day(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_index_quant_data_his(code = None, start_date = '2008-01-01', end_date = QA_util_today_str()):
    save_stock_quant.QA_SU_save_index_quant_day(code = code, start_date = start_date ,end_date = end_date)

def QA_SU_save_index_alpha101_day(code = None, start_date = QA_util_today_str(), end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_index_alpha101_day(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_stock_alpha101_day(code = None, start_date = QA_util_today_str(), end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_stock_alpha101_day(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_index_alpha101_his(code = None, start_date = '2008-01-01', end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_index_alpha101_day(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_stock_alpha101_his(code = None, start_date = '2008-01-01', end_date = QA_util_today_str()):
    save_stock_alpha.QA_SU_save_stock_alpha101_his(code = code, start_date = start_date, end_date = end_date)

def QA_SU_save_index_info():
    save_tdx.QA_SU_save_index_info()

def QA_SU_save_stock_industryinfo():
    save_tdx.QA_SU_save_stock_industryinfo()

def QA_SU_save_stock_delist():
    save_tdx.QA_SU_save_stock_delist()

def QA_SU_save_index_week():
    save_tdx.QA_SU_save_index_week()

def QA_SU_save_index_month():
    save_tdx.QA_SU_save_index_month()

def QA_SU_save_index_year():
    save_tdx.QA_SU_save_index_year()

def QA_SU_save_stock_day():
    save_tdx.QA_SU_save_stock_day()

def QA_SU_save_stock_xdxr():
    save_tdx.QA_SU_save_stock_xdxr()

def QA_SU_save_stock_info():
    save_tdx.QA_SU_save_stock_info()