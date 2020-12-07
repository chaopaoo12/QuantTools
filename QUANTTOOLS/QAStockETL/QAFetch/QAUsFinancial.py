from QUANTTOOLS.QAStockETL.Crawly import read_financial_report,read_stock_day
from QUANTAXIS.QAUtil import QA_util_date_stamp
import datetime
import pandas as pd

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
    if code[0:2] == '60':
        code = 'SH'+code
    elif code[0:3] in ['000','002','300']:
        code = 'SZ'+code
    else:
        code = code
    data = read_stock_day(code, start_date, end_date, period, type)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(code=data['code'].apply(lambda x: str(x)[2:]))
    return(data)
