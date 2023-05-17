from QUANTTOOLS.QAStockETL.Crawly import read_financial_report,read_stock_day
from akshare import stock_zh_a_minute,stock_zh_a_hist_min_em
from QUANTAXIS.QAUtil import QA_util_date_stamp
import datetime
import pandas as pd
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import trans_code
import multiprocessing
from functools import partial
import random

def QA_fetch_get_stock_report_xq(code):
    data = read_financial_report(code)
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_usstock_report_xq(code):
    income = read_financial_report(code, 'us', 'income').set_index(['report_date','report_name','report_type_code','report_annual'])
    balance = read_financial_report(code, 'us', 'balance').set_index(['report_date','report_name','report_type_code','report_annual'])
    cashflow = read_financial_report(code, 'us', 'cash_flow').set_index(['report_date','report_name','report_type_code','report_annual'])
    data = pd.concat([balance,income,cashflow],axis=1)[['accrued_liab', 'total_cash', 'income_tax_payable', 'nca_si',
                                                        'total_equity_special_subject', 'treasury_stock', 'total_liab',
                                                        'preferred_stock', 'dt_assets_current_assets', 'cce',
                                                        'equity_and_othr_invest', 'net_intangible_assets', 'minority_interest',
                                                        'accum_amortization', 'accum_othr_compre_income', 'total_current_liab',
                                                        'current_assets_special_subject', 'dt_assets_noncurrent_assets',
                                                        'total_holders_equity_si', 'deferred_tax_liab',
                                                        'net_property_plant_and_equip', 'noncurrent_liab_si', 'total_assets',
                                                        'total_equity', 'inventory', 'st_debt', 'dr_noncurrent_liab',
                                                        'goodwill', 'total_assets_special_subject', 'common_stock',
                                                        'total_noncurrent_liab', 'gross_property_plant_and_equip',
                                                        'deferred_revenue_current_liab', 'total_holders_equity',
                                                        'total_current_assets', 'asset_liab_ratio', 'total_liab_si',
                                                        'accounts_payable', 'retained_earning', 'st_invest', 'lt_debt',
                                                        'total_noncurrent_assets', 'net_receivables', 'current_liab_si',
                                                        'add_paid_in_capital', 'prepaid_expense', 'accum_depreciation',
                                                        'marketing_selling_etc', 'total_dlt_earnings_common_ps',
                                                        'total_revenue', 'total_operate_expenses', 'net_income_atms_interest',
                                                        'income_from_co_before_tax_si', 'income_tax', 'income_from_co',
                                                        'gross_profit', 'interest_expense', 'sales_cost', 'interest_income',
                                                        'operating_income', 'rad_expenses', 'net_interest_expense',
                                                        'total_net_income_atcss', 'share_of_earnings_of_affiliate',
                                                        'total_compre_income_atms', 'othr_revenues',
                                                        'net_income_atcss', 'total_basic_earning_common_ps',
                                                        'total_compre_income', 'revenue', 'total_operate_expenses_si',
                                                        'net_income', 'income_from_co_before_it', 'preferred_dividend',
                                                        'total_compre_income_atcss',
                                                        'depreciation_and_amortization', 'effect_of_exchange_chg_on_cce',
                                                        'operating_asset_and_liab_chg', 'repur_of_common_stock',
                                                        'increase_in_cce', 'payment_for_property_and_equip',
                                                        'common_stock_issue', 'cce_at_boy', 'dividend_paid',
                                                        'net_cash_provided_by_oa', 'cce_at_eoy', 'net_cash_used_in_fa',
                                                        'net_cash_used_in_ia', 'purs_of_invest',
                                                        'ctime', 'sd', 'ed']].reset_index()
    data = data.assign(ctime = data.ctime.apply(lambda x:str(datetime.datetime.fromtimestamp(x/1000))[0:10]))
    #data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    #data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_usstock_day_xq(code, start_date, end_date, period='day', type='normal'):
    if isinstance(code, list):
        code1 = [trans_code(i) for i in code]
    else:
        code1 = [trans_code(code)]
    data = read_stock_day(code1, start_date, end_date, period, type)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(code=code)
    return(data)

def proxy_stock_zh_a_hist_min_em(symbol_proxies, period, adjust):
    try:
        res = stock_zh_a_hist_min_em(symbol=symbol_proxies[1], period=period, adjust=adjust, proxies=symbol_proxies[0])
        res = res.assign(code=symbol_proxies[1])
        return(res)
    except:
        return(None)

def QA_fetch_get_stock_min_sina(code, period='30', type='',proxies=None):

    if isinstance(code,list):
        if isinstance(proxies, list):
            if len(proxies) > len(code):
                proxies = proxies[0:len(code)]
            symbol_proxies = list(zip(random.choices(proxies, k=len(code)),code))
            pool = multiprocessing.Pool(20)
            with pool as p:
                res = p.map(partial(proxy_stock_zh_a_hist_min_em, period=period, adjust=type), symbol_proxies)
        else:
            pool = multiprocessing.Pool(20)
            with pool as p:
                res = p.map(partial(stock_zh_a_hist_min_em, period=period, adjust=type), code)

        data = pd.concat(res,axis=0)

    elif isinstance(code, str):
        data = stock_zh_a_hist_min_em(symbol=code, period=period, adjust=type)
        data = data.assign(code=code)
    else:
        data=None
    try:
        data = data.rename(columns={'时间':'datetime',
                                    '开盘':'open',
                                    '收盘':'close',
                                    '最高':'high',
                                    '最低':'low',
                                    '成交量':'volume',
                                    '成交额':'amount',
                                    '最新价':'price',})
        data[['open','close','high','low','volume','amount']] = data[['open','close','high','low','volume','amount']].apply(pd.to_numeric)
        #data = data.assign(pct=data.pct/100)
        data = data.assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        data['datetime']=pd.to_datetime(data['datetime'],format='%Y-%m-%d %H:%M:%S')
    except:
        data = None

    try:
        data[['pct_chg']] = data[['pct_chg']].apply(pd.to_numeric)
    except:
        pass
    return(data)

def QA_fetch_get_index_min_sina(code,period='30'):
    #code = ['sh000001','sz399001','sz399006','sz399005']
    #if code[0:2] == '00' and len(code) == 6:
    #    code1 = 'SH'+code
    #elif code[0:2] == '39':
    #    code1 = 'SZ'+code
    #else:
    #    code1 = code

    data = stock_zh_a_hist_min_em(symbol=code, period=period)
    try:
        data = data.rename(columns={'时间':'datetime',
                                    '开盘':'open',
                                    '收盘':'close',
                                    '最高':'high',
                                    '最低':'low',
                                    '成交量':'volume',
                                    '成交额':'amount',
                                    '最新价':'price',})
        data[['open','close','high','low','volume','amount','price']] = data[['open','close','high','low','volume','amount','price']].apply(pd.to_numeric)
        data = data.assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),code=code)
        data['datetime']=pd.to_datetime(data['datetime'],format='%Y-%m-%d %H:%M:%S')
    except:
        data = None
    return(data)

if __name__ == '__main__':
    pass