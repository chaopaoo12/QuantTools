import cx_Oracle
import pandas as pd
import datetime

from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)

def QA_util_process_quantdata(start_date = None, end_date = None):

    if start_date is None:
        start_date = QA_util_today_str()
        end_date = QA_util_today_str()
    elif end_date is None:
        end_date = start_date

    sql1 = """INSERT INTO QUANT_ANALYSIS_DATA
  select *
    from (select CODE,
   ORDER_DATE,
   REPORT_DATE,
   MARKET_DAY,
   LASTYEAR,
   LAST2YEAR,
   LAST3YEAR,
   LAST4YEAR,
   LAST5YEAR,
   LAG1,
   SEND_DATE,
   END_DATE,
   INDUSTRY,
   NAME,
   AREA,
   OPEN,
   HIGH,
   LOW,
   CLOSE,
   VOL,
   AMOUNT,
   AVGRAGE,
   OPEN_QFQ,
   HIGH_QFQ,
   LOW_QFQ,
   CLOSE_QFQ,
   VOL_QFQ,
   AMOUNT_QFQ,
   AVGRAGE_QFQ,
   SHARES,
   TRA_SHARES,
   TURNOVERRATIO,
   TOTAL_MARKET,
   TRA_TOTAL_MARKET,
   OPEN_MARKET,
   HIGH_MARKET,
   LOW_MARKET,
   AVG_TOTAL_MARKET,
   PE,
   PB,
   TOTALASSETS,
   AVGTOTALASSETS,
   FIXEDASSETS,
   AVGFIXEDASSETS,
   GOODWILL,
   AVGGOODWILL,
   INVENTORY,
   MONEYFUNDS,
   ACCOUNTSPAYABLE,
   AVGACCOUNTSPAYABLE,
   AVGINVENTORY,
   TOTALLIQUIDASSETS,
   AVGTOTALLIQUIDASSETS,
   TOTALLIABILITIES,
   AVGTOTALLIABILITIES,
   ACCOUNTSRECEIVABLES,
   AVGACCOUNTSRECEIVABLES,
   INTERCOMPANYRECEIVABLES,
   AVGINTERCOMPANYRECEIVABLES,
   PREPAYMENTS,
   AVGPREPAYMENTS,
   TOTALCURRENTLIABILITIES,
   AVGTOTALCURRENTLIABILITIES,
   NETCASHOPERATACTIV,
   CASHOUTINVESTACTIV,
   NETPROFIT,
   OPERATINGREVENUE_TTM,
   OPERATINGCOSTS_TTM,
   TAXANDSURCHARGES_TTM,
   SALESCOSTS_TTM,
   MANAGEMENTCOSTS_TTM,
   EXPLORATIONCOSTS_TTM,
   FINANCIALCOSTS_TTM,
   ASSESTSDEVALUATION_TTM,
   OPERATINGPROFIT_TTM,
   TOTALPROFIT_TTM,
   INCOMETAX_TTM,
   NETPROFIT_TTM,
   NETPROAFTEXTRGAINLOSS_TTM,
   INTEREST_TTM,
   DEPRECFORFIXEDASSETS_TTM,
   NETCASHOPERATACTIV_TTM,
   CASHOUTINVESTACTIV_TTM,
   ROE,
   GROSSMARGIN,
   ROA,
   NETPROFITMARGINONSALES_TTM,
   INCOMETAXRATIO,
   REINVESTEDINCOMERATIO,
   DEPRECIATIONRATIO,
   OPERATINGCASHRATIO_TTM,
   AVGFIEXDOFASSETS,
   FIEXDOFASSETS,
   ACIDTESTRATIO,
   TURNOVERRATIOOFRECEIVABLE,
   TURNOVERRATIOOFINVENTORY,
   TURNOVERRATIOOFTOTALASSETS,
   DEPRECIATIONOFTOTALCOSTS,
   CASHOFNETPROFIT,
   CASHOFNETPROFIT_TTM,
   CASHOFINTEREST,
   ASSETSLIABILITIESRATIO,
   TANGIBLEASSETDEBTRATIO,
   CASHRATIO,
   TOTALASSETS_LY,
   AVGTOTALASSETS_LY,
   FIXEDASSETS_LY,
   AVGFIXEDASSETS_LY,
   GOODWILL_LY,
   AVGGOODWILL_LY,
   INVENTORY_LY,
   MONEYFUNDS_LY,
   ACCOUNTSPAYABLE_LY,
   AVGACCOUNTSPAYABLE_LY,
   AVGINVENTORY_LY,
   TOTALLIQUIDASSETS_LY,
   AVGTOTALLIQUIDASSETS_LY,
   TOTALLIABILITIES_LY,
   AVGTOTALLIABILITIES_LY,
   ACCOUNTSRECEIVABLES_LY,
   AVGACCOUNTSRECEIVABLES_LY,
   INTERCOMPANYRECEIVABLES_LY,
   AVGINTERCOMPANYRECEIVABLES_LY,
   PREPAYMENTS_LY,
   AVGPREPAYMENTS_LY,
   TOTALCURRENTLIABILITIES_LY,
   AVGTOTALCURRENTLIABILITIES_LY,
   NETCASHOPERATACTIV_LY,
   CASHOUTINVESTACTIV_LY,
   NETPROFIT_LY,
   OPERATINGREVENUE_TTM_LY,
   OPERATINGCOSTS_TTM_LY,
   TAXANDSURCHARGES_TTM_LY,
   SALESCOSTS_TTM_LY,
   MANAGEMENTCOSTS_TTM_LY,
   EXPLORATIONCOSTS_TTM_LY,
   FINANCIALCOSTS_TTM_LY,
   ASSESTSDEVALUATION_TTM_LY,
   OPERATINGPROFIT_TTM_LY,
   TOTALPROFIT_TTM_LY,
   INCOMETAX_TTM_LY,
   NETPROFIT_TTM_LY,
   NETPROAFTEXTRGAINLOSS_TTM_LY,
   INTEREST_TTM_LY,
   DEPRECFORFIXEDASSETS_TTM_LY,
   NETCASHOPERATACTIV_TTM_LY,
   CASHOUTINVESTACTIV_TTM_LY,
   ROE_LY,
   GROSSMARGIN_LY,
   ROA_LY,
   NETPROFITMARGINONSALES_TTM_LY,
   INCOMETAXRATIO_LY,
   REINVESTEDINCOMERATIO_LY,
   DEPRECIATIONRATIO_LY,
   OPERATINGCASHRATIO_TTM_LY,
   AVGFIEXDOFASSETS_LY,
   FIEXDOFASSETS_LY,
   ACIDTESTRATIO_LY,
   TURNOVERRATIOOFRECEIVABLE_LY,
   TURNOVERRATIOOFINVENTORY_LY,
   TURNOVERRATIOOFTOTALASSETS_LY,
   DEPRECIATIONOFTOTALCOSTS_LY,
   CASHOFNETPROFIT_LY,
   CASHOFNETPROFIT_TTM_LY,
   CASHOFINTEREST_LY,
   ASSETSLIABILITIESRATIO_LY,
   TANGIBLEASSETDEBTRATIO_LY,
   CASHRATIO_LY,
   TOTALASSETS_L2Y,
   AVGTOTALASSETS_L2Y,
   FIXEDASSETS_L2Y,
   AVGFIXEDASSETS_L2Y,
   GOODWILL_L2Y,
   AVGGOODWILL_L2Y,
   INVENTORY_L2Y,
   MONEYFUNDS_L2Y,
   ACCOUNTSPAYABLE_L2Y,
   AVGACCOUNTSPAYABLE_L2Y,
   AVGINVENTORY_L2Y,
   TOTALLIQUIDASSETS_L2Y,
   AVGTOTALLIQUIDASSETS_L2Y,
   TOTALLIABILITIES_L2Y,
   AVGTOTALLIABILITIES_L2Y,
   ACCOUNTSRECEIVABLES_L2Y,
   AVGACCOUNTSRECEIVABLES_L2Y,
   INTERCOMPANYRECEIVABLES_L2Y,
   AVGINTERCOMPANYRECEIVABLES_L2Y,
   PREPAYMENTS_L2Y,
   AVGPREPAYMENTS_L2Y,
   TOTALCURRENTLIABILITIES_L2Y,
   AVGTOTALCURRENTLIABILITIES_L2Y,
   NETCASHOPERATACTIV_L2Y,
   CASHOUTINVESTACTIV_L2Y,
   NETPROFIT_L2Y,
   OPERATINGREVENUE_TTM_L2Y,
   OPERATINGCOSTS_TTM_L2Y,
   TAXANDSURCHARGES_TTM_L2Y,
   SALESCOSTS_TTM_L2Y,
   MANAGEMENTCOSTS_TTM_L2Y,
   EXPLORATIONCOSTS_TTM_L2Y,
   FINANCIALCOSTS_TTM_L2Y,
   ASSESTSDEVALUATION_TTM_L2Y,
   OPERATINGPROFIT_TTM_L2Y,
   TOTALPROFIT_TTM_L2Y,
   INCOMETAX_TTM_L2Y,
   NETPROFIT_TTM_L2Y,
   NETPROAFTEXTRGAINLOSS_TTM_L2Y,
   INTEREST_TTM_L2Y,
   DEPRECFORFIXEDASSETS_TTM_L2Y,
   NETCASHOPERATACTIV_TTM_L2Y,
   CASHOUTINVESTACTIV_TTM_L2Y,
   ROE_L2Y,
   GROSSMARGIN_L2Y,
   ROA_L2Y,
   NETPROFITMARGINONSALES_TTM_L2Y,
   INCOMETAXRATIO_L2Y,
   REINVESTEDINCOMERATIO_L2Y,
   DEPRECIATIONRATIO_L2Y,
   OPERATINGCASHRATIO_TTM_L2Y,
   AVGFIEXDOFASSETS_L2Y,
   FIEXDOFASSETS_L2Y,
   ACIDTESTRATIO_L2Y,
   TURNOVERRATIOOFRECEIVABLE_L2Y,
   TURNOVERRATIOOFINVENTORY_L2Y,
   TURNOVERRATIOOFTOTALASSETS_L2Y,
   DEPRECIATIONOFTOTALCOSTS_L2Y,
   CASHOFNETPROFIT_L2Y,
   CASHOFNETPROFIT_TTM_L2Y,
   CASHOFINTEREST_L2Y,
   ASSETSLIABILITIESRATIO_L2Y,
   TANGIBLEASSETDEBTRATIO_L2Y,
   CASHRATIO_L2Y,
   TOTALASSETS_L3Y,
   AVGTOTALASSETS_L3Y,
   FIXEDASSETS_L3Y,
   AVGFIXEDASSETS_L3Y,
   GOODWILL_L3Y,
   AVGGOODWILL_L3Y,
   INVENTORY_L3Y,
   MONEYFUNDS_L3Y,
   ACCOUNTSPAYABLE_L3Y,
   AVGACCOUNTSPAYABLE_L3Y,
   AVGINVENTORY_L3Y,
   TOTALLIQUIDASSETS_L3Y,
   AVGTOTALLIQUIDASSETS_L3Y,
   TOTALLIABILITIES_L3Y,
   AVGTOTALLIABILITIES_L3Y,
   ACCOUNTSRECEIVABLES_L3Y,
   AVGACCOUNTSRECEIVABLES_L3Y,
   INTERCOMPANYRECEIVABLES_L3Y,
   AVGINTERCOMPANYRECEIVABLES_L3Y,
   PREPAYMENTS_L3Y,
   AVGPREPAYMENTS_L3Y,
   TOTALCURRENTLIABILITIES_L3Y,
   AVGTOTALCURRENTLIABILITIES_L3Y,
   NETCASHOPERATACTIV_L3Y,
   CASHOUTINVESTACTIV_L3Y,
   NETPROFIT_L3Y,
   OPERATINGREVENUE_TTM_L3Y,
   OPERATINGCOSTS_TTM_L3Y,
   TAXANDSURCHARGES_TTM_L3Y,
   SALESCOSTS_TTM_L3Y,
   MANAGEMENTCOSTS_TTM_L3Y,
   EXPLORATIONCOSTS_TTM_L3Y,
   FINANCIALCOSTS_TTM_L3Y,
   ASSESTSDEVALUATION_TTM_L3Y,
   OPERATINGPROFIT_TTM_L3Y,
   TOTALPROFIT_TTM_L3Y,
   INCOMETAX_TTM_L3Y,
   NETPROFIT_TTM_L3Y,
   NETPROAFTEXTRGAINLOSS_TTM_L3Y,
   INTEREST_TTM_L3Y,
   DEPRECFORFIXEDASSETS_TTM_L3Y,
   NETCASHOPERATACTIV_TTM_L3Y,
   CASHOUTINVESTACTIV_TTM_L3Y,
   ROE_L3Y,
   GROSSMARGIN_L3Y,
   ROA_L3Y,
   NETPROFITMARGINONSALES_TTM_L3Y,
   INCOMETAXRATIO_L3Y,
   REINVESTEDINCOMERATIO_L3Y,
   DEPRECIATIONRATIO_L3Y,
   OPERATINGCASHRATIO_TTM_L3Y,
   AVGFIEXDOFASSETS_L3Y,
   FIEXDOFASSETS_L3Y,
   ACIDTESTRATIO_L3Y,
   TURNOVERRATIOOFRECEIVABLE_L3Y,
   TURNOVERRATIOOFINVENTORY_L3Y,
   TURNOVERRATIOOFTOTALASSETS_L3Y,
   DEPRECIATIONOFTOTALCOSTS_L3Y,
   CASHOFNETPROFIT_L3Y,
   CASHOFNETPROFIT_TTM_L3Y,
   CASHOFINTEREST_L3Y,
   ASSETSLIABILITIESRATIO_L3Y,
   TANGIBLEASSETDEBTRATIO_L3Y,
   CASHRATIO_L3Y,
   TOTALASSETS_L4Y,
   AVGTOTALASSETS_L4Y,
   FIXEDASSETS_L4Y,
   AVGFIXEDASSETS_L4Y,
   GOODWILL_L4Y,
   AVGGOODWILL_L4Y,
   INVENTORY_L4Y,
   MONEYFUNDS_L4Y,
   ACCOUNTSPAYABLE_L4Y,
   AVGACCOUNTSPAYABLE_L4Y,
   AVGINVENTORY_L4Y,
   TOTALLIQUIDASSETS_L4Y,
   AVGTOTALLIQUIDASSETS_L4Y,
   TOTALLIABILITIES_L4Y,
   AVGTOTALLIABILITIES_L4Y,
   ACCOUNTSRECEIVABLES_L4Y,
   AVGACCOUNTSRECEIVABLES_L4Y,
   INTERCOMPANYRECEIVABLES_L4Y,
   AVGINTERCOMPANYRECEIVABLES_L4Y,
   PREPAYMENTS_L4Y,
   AVGPREPAYMENTS_L4Y,
   TOTALCURRENTLIABILITIES_L4Y,
   AVGTOTALCURRENTLIABILITIES_L4Y,
   NETCASHOPERATACTIV_L4Y,
   CASHOUTINVESTACTIV_L4Y,
   NETPROFIT_L4Y,
   OPERATINGREVENUE_TTM_L4Y,
   OPERATINGCOSTS_TTM_L4Y,
   TAXANDSURCHARGES_TTM_L4Y,
   SALESCOSTS_TTM_L4Y,
   MANAGEMENTCOSTS_TTM_L4Y,
   EXPLORATIONCOSTS_TTM_L4Y,
   FINANCIALCOSTS_TTM_L4Y,
   ASSESTSDEVALUATION_TTM_L4Y,
   OPERATINGPROFIT_TTM_L4Y,
   TOTALPROFIT_TTM_L4Y,
   INCOMETAX_TTM_L4Y,
   NETPROFIT_TTM_L4Y,
   NETPROAFTEXTRGAINLOSS_TTM_L4Y,
   INTEREST_TTM_L4Y,
   DEPRECFORFIXEDASSETS_TTM_L4Y,
   NETCASHOPERATACTIV_TTM_L4Y,
   CASHOUTINVESTACTIV_TTM_L4Y,
   ROE_L4Y,
   GROSSMARGIN_L4Y,
   ROA_L4Y,
   NETPROFITMARGINONSALES_TTM_L4Y,
   INCOMETAXRATIO_L4Y,
   REINVESTEDINCOMERATIO_L4Y,
   DEPRECIATIONRATIO_L4Y,
   OPERATINGCASHRATIO_TTM_L4Y,
   AVGFIEXDOFASSETS_L4Y,
   FIEXDOFASSETS_L4Y,
   ACIDTESTRATIO_L4Y,
   TURNOVERRATIOOFRECEIVABLE_L4Y,
   TURNOVERRATIOOFINVENTORY_L4Y,
   TURNOVERRATIOOFTOTALASSETS_L4Y,
   DEPRECIATIONOFTOTALCOSTS_L4Y,
   CASHOFNETPROFIT_L4Y,
   CASHOFNETPROFIT_TTM_L4Y,
   CASHOFINTEREST_L4Y,
   ASSETSLIABILITIESRATIO_L4Y,
   TANGIBLEASSETDEBTRATIO_L4Y,
   CASHRATIO_L4Y,
   TOTALASSETS_L5Y,
   AVGTOTALASSETS_L5Y,
   FIXEDASSETS_L5Y,
   AVGFIXEDASSETS_L5Y,
   GOODWILL_L5Y,
   AVGGOODWILL_L5Y,
   INVENTORY_L5Y,
   MONEYFUNDS_L5Y,
   ACCOUNTSPAYABLE_L5Y,
   AVGACCOUNTSPAYABLE_L5Y,
   AVGINVENTORY_L5Y,
   TOTALLIQUIDASSETS_L5Y,
   AVGTOTALLIQUIDASSETS_L5Y,
   TOTALLIABILITIES_L5Y,
   AVGTOTALLIABILITIES_L5Y,
   ACCOUNTSRECEIVABLES_L5Y,
   AVGACCOUNTSRECEIVABLES_L5Y,
   INTERCOMPANYRECEIVABLES_L5Y,
   AVGINTERCOMPANYRECEIVABLES_L5Y,
   PREPAYMENTS_L5Y,
   AVGPREPAYMENTS_L5Y,
   TOTALCURRENTLIABILITIES_L5Y,
   AVGTOTALCURRENTLIABILITIES_L5Y,
   NETCASHOPERATACTIV_L5Y,
   CASHOUTINVESTACTIV_L5Y,
   NETPROFIT_L5Y,
   OPERATINGREVENUE_TTM_L5Y,
   OPERATINGCOSTS_TTM_L5Y,
   TAXANDSURCHARGES_TTM_L5Y,
   SALESCOSTS_TTM_L5Y,
   MANAGEMENTCOSTS_TTM_L5Y,
   EXPLORATIONCOSTS_TTM_L5Y,
   FINANCIALCOSTS_TTM_L5Y,
   ASSESTSDEVALUATION_TTM_L5Y,
   OPERATINGPROFIT_TTM_L5Y,
   TOTALPROFIT_TTM_L5Y,
   INCOMETAX_TTM_L5Y,
   NETPROFIT_TTM_L5Y,
   NETPROAFTEXTRGAINLOSS_TTM_L5Y,
   INTEREST_TTM_L5Y,
   DEPRECFORFIXEDASSETS_TTM_L5Y,
   NETCASHOPERATACTIV_TTM_L5Y,
   CASHOUTINVESTACTIV_TTM_L5Y,
   ROE_L5Y,
   GROSSMARGIN_L5Y,
   ROA_L5Y,
   NETPROFITMARGINONSALES_TTM_L5Y,
   INCOMETAXRATIO_L5Y,
   REINVESTEDINCOMERATIO_L5Y,
   DEPRECIATIONRATIO_L5Y,
   OPERATINGCASHRATIO_TTM_L5Y,
   AVGFIEXDOFASSETS_L5Y,
   FIEXDOFASSETS_L5Y,
   ACIDTESTRATIO_L5Y,
   TURNOVERRATIOOFRECEIVABLE_L5Y,
   TURNOVERRATIOOFINVENTORY_L5Y,
   TURNOVERRATIOOFTOTALASSETS_L5Y,
   DEPRECIATIONOFTOTALCOSTS_L5Y,
   CASHOFNETPROFIT_L5Y,
   CASHOFNETPROFIT_TTM_L5Y,
   CASHOFINTEREST_L5Y,
   ASSETSLIABILITIESRATIO_L5Y,
   TANGIBLEASSETDEBTRATIO_L5Y,
   CASHRATIO_L5Y,
   TOTALASSETS_LQ,
   AVGTOTALASSETS_LQ,
   FIXEDASSETS_LQ,
   AVGFIXEDASSETS_LQ,
   GOODWILL_LQ,
   AVGGOODWILL_LQ,
   INVENTORY_LQ,
   MONEYFUNDS_LQ,
   ACCOUNTSPAYABLE_LQ,
   AVGACCOUNTSPAYABLE_LQ,
   AVGINVENTORY_LQ,
   TOTALLIQUIDASSETS_LQ,
   AVGTOTALLIQUIDASSETS_LQ,
   TOTALLIABILITIES_LQ,
   AVGTOTALLIABILITIES_LQ,
   ACCOUNTSRECEIVABLES_LQ,
   AVGACCOUNTSRECEIVABLES_LQ,
   INTERCOMPANYRECEIVABLES_LQ,
   AVGINTERCOMPANYRECEIVABLES_LQ,
   PREPAYMENTS_LQ,
   AVGPREPAYMENTS_LQ,
   TOTALCURRENTLIABILITIES_LQ,
   AVGTOTALCURRENTLIABILITIES_LQ,
   NETCASHOPERATACTIV_LQ,
   CASHOUTINVESTACTIV_LQ,
   NETPROFIT_LQ,
   OPERATINGREVENUE_TTM_LQ,
   OPERATINGCOSTS_TTM_LQ,
   TAXANDSURCHARGES_TTM_LQ,
   SALESCOSTS_TTM_LQ,
   MANAGEMENTCOSTS_TTM_LQ,
   EXPLORATIONCOSTS_TTM_LQ,
   FINANCIALCOSTS_TTM_LQ,
   ASSESTSDEVALUATION_TTM_LQ,
   OPERATINGPROFIT_TTM_LQ,
   TOTALPROFIT_TTM_LQ,
   INCOMETAX_TTM_LQ,
   NETPROFIT_TTM_LQ,
   NETPROAFTEXTRGAINLOSS_TTM_LQ,
   INTEREST_TTM_LQ,
   DEPRECFORFIXEDASSETS_TTM_LQ,
   NETCASHOPERATACTIV_TTM_LQ,
   CASHOUTINVESTACTIV_TTM_LQ,
   ROE_LQ,
   GROSSMARGIN_LQ,
   ROA_LQ,
   NETPROFITMARGINONSALES_TTM_LQ,
   INCOMETAXRATIO_LQ,
   REINVESTEDINCOMERATIO_LQ,
   DEPRECIATIONRATIO_LQ,
   OPERATINGCASHRATIO_TTM_LQ,
   AVGFIEXDOFASSETS_LQ,
   FIEXDOFASSETS_LQ,
   ACIDTESTRATIO_LQ,
   TURNOVERRATIOOFRECEIVABLE_LQ,
   TURNOVERRATIOOFINVENTORY_LQ,
   TURNOVERRATIOOFTOTALASSETS_LQ,
   DEPRECIATIONOFTOTALCOSTS_LQ,
   CASHOFNETPROFIT_LQ,
   CASHOFNETPROFIT_TTM_LQ,
   CASHOFINTEREST_LQ,
   ASSETSLIABILITIESRATIO_LQ,
   TANGIBLEASSETDEBTRATIO_LQ,
   CASHRATIO_LQ,
   I_TOTAL_MARKET,
   I_NETPROAFTEXTRGAINLOSS_TTM,
   I_AVGTOTALASSETS,
   I_AVGGOODWILL,
   I_AVGTOTALLIABILITIES,
   I_OPERATINGREVENUE_TTM,
   INDUSTRYOPERATINGCOSTS_TTM,
   ALL_TOTAL_MARKET,
   ALL_NETPROAFTEXTRGAINLOSS_TTM,
   ALL_AVGTOTALASSETS,
   ALL_AVGGOODWILL,
   ALL_AVGTOTALLIABILITIES,
   ALL_OPERATINGREVENUE_TTM,
   ALL_OPERATINGCOSTS_TTM,
   LAG_MARKET,
   LAG2_MARKET,
   LAG3_MARKET,
   LAG5_MARKET,
   LAG20_MARKET,
   LAG30_MARKET,
   LAG60_MARKET,
   AVG_LAG_MARKET,
   AVG_LAG2_MARKET,
   AVG_LAG3_MARKET,
   AVG_LAG5_MARKET,
   AVG_LAG20_MARKET,
   AVG_LAG30_MARKET,
   AVG_LAG60_MARKET,
   AVG5_T_MARKET,
   AVG20_T_MARKET,
   AVG30_T_MARKET,
   AVG60_T_MARKET,
   AVG5_A_MARKET,
   AVG20_A_MARKET,
   AVG30_A_MARKET,
   AVG60_A_MARKET,
   AVG5_TOR,
   AVG20_TOR,
   AVG30_TOR,
   AVG60_TOR,
   RNG_60,
   RNG_30,
   RNG_20,
   RNG_5,
   RNG_L,
   ALL_AMOUNT,
   I_PE_TOTAL,
   I_PB_TOTAL,
   I_ROE_TOTAL,
   I_GROSSMARGIN_TOTAL,
   I_ROA_TOTAL,
   I_PE,
   I_PB,
   I_GROSSMARGIN,
   I_ROE,
   I_ROA,
   ALL_PE_TOTAL,
   ALL_PB_TOTAL,
   ALL_ROE_TOTAL,
   ALL_GROSSMARGIN_TOTAL,
   ALL_ROA_TOTAL,
   ALL_PE,
   ALL_PB,
   ALL_GROSSMARGIN,
   ALL_ROE,
   ALL_ROA,
   AVG5_AT_MARKET,
   AVG20_AT_MARKET,
   AVG30_AT_MARKET,
   AVG60_AT_MARKET,
   AVG5_AMOUNT,
   AVG20_AMOUNT,
   AVG30_AMOUNT,
   AVG60_AMOUNT,
   LAG_ALL_MARKET,
   LAG2_ALL_MARKET,
   LAG3_ALL_MARKET,
   LAG5_ALL_MARKET,
   LAG20_ALL_MARKET,
   LAG30_ALL_MARKET,
   LAG60_ALL_MARKET,
   LAG(TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE_MARKET,
   LAG(TOTAL_MARKET, 2) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE2_MARKET,
   LAG(TOTAL_MARKET, 3) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE3_MARKET,
   LAG(TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE5_MARKET,
   LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE_MARKET,
   LAG(AVG_TOTAL_MARKET, 2) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE2_MARKET,
   LAG(AVG_TOTAL_MARKET, 3) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE3_MARKET,
   LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE5_MARKET
            from (select *,
                    from stock_analysis_data a where order_date <= (to_date('{end_date}', 'yyyy-mm-dd'))
           and order_date >= to_date('{start_date}', 'yyyy-mm-dd')
                    ) A
        where ORDER_DATE = '{start_date}'"""

    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    cursor = conn.cursor()
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    while start_date <= end_date:
        print(start_date)
        if QA_util_if_trade(start_date):
            print(start_date.strftime("%Y-%m-%d"), QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5))
            sql2 = sql1.format(start_date=QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5), end_date = start_date)
            if QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5) == 'wrong date':
                print(sql2)
            else:
                cursor.execute(sql2)
            print('quant analysis data for {deal_date} has been stored'.format(deal_date=start_date.strftime("%Y-%m-%d")))
            conn.commit()
        start_date = start_date+datetime.timedelta(days=1)

    cursor.close()
    conn.close()

def QA_util_etl_stock_quant(deal_date = None):

    sql = '''select A.code,
       name,
       industry,
       to_char(order_date, 'yyyy-mm-dd') as "date",
       order_Date - market_day as days,
       total_market,
       SZ50,
       HS300,
       CY300,
       SZ180,
       SZ380,
       SZ100,
       SZ300,
       ZZ100,
       ZZ200,
       CY50,
       round(tra_total_market / total_market * 100, 2) AS tra_rate,
       round(pe, 2) AS pe,
       round(pb, 2) AS pb,
       round(roe * 100, 2) AS roe,
       round(roe_ly * 100, 2) AS roe_ly,
       round(roe_l2y * 100, 2) AS roe_l2y,
       round(roe_l3y * 100, 2) AS roe_l3y,
       round(roe_l4y * 100, 2) AS roe_l4y,
       round(roa * 100, 2) AS roa,
       round(roa_ly * 100, 2) AS roa_ly,
       round(roa_l2y * 100, 2) AS roa_l2y,
       round(roa_l3y * 100, 2) AS roa_l3y,
       round(roa_l4y * 100, 2) AS roa_l4y,
       round(grossMargin * 100, 2) AS grossMargin,
       round(grossMargin_ly * 100, 2) AS grossMargin_ly,
       round(grossMargin_l2y * 100, 2) AS grossMargin_l2y,
       round(grossMargin_l3y * 100, 2) AS grossMargin_l3y,
       round(grossMargin_l4y * 100, 2) AS grossMargin_l4y,
       round(assetsLiabilitiesRatio * 100, 2) AS assetsLiabilitiesRatio,
       round(assetsLiabilitiesRatio_ly * 100, 2) AS assetsLiabilitiesRatio_ly,
       round(assetsLiabilitiesRatio_l2y * 100, 2) AS assetsLiabilitiesRatio_l2y,
       round(assetsLiabilitiesRatio_l3y * 100, 2) AS assetsLiabilitiesRatio_l3y,
       round(assetsLiabilitiesRatio_l4y * 100, 2) AS assetsLiabilitiesRatio_l4y,
       round(cashRatio * 100, 2) AS cashRatio,
       round(cashRatio_ly * 100, 2) AS cashRatio_ly,
       round(cashRatio_l2y * 100, 2) AS cashRatio_l2y,
       round(cashRatio_l3y * 100, 2) AS cashRatio_l3y,
       round(cashRatio_l4y * 100, 2) AS cashRatio_l4y,
       round(tangibleAssetDebtRatio * 100, 2) AS tangibleAssetDebtRatio,
       round(tangibleAssetDebtRatio_ly * 100, 2) AS tangibleAssetDebtRatio_ly,
       round(tangibleAssetDebtRatio_l2y * 100, 2) AS tangibleAssetDebtRatio_l2y,
       round(tangibleAssetDebtRatio_l3y * 100, 2) AS tangibleAssetDebtRatio_l3y,
       round(tangibleAssetDebtRatio_l4y * 100, 2) AS tangibleAssetDebtRatio_l4y,
       
       round(turnoverRatioOfTotalAssets * 100, 2) AS turnoverRatioOfTotalAssets,
       round(turnoverRatioOfTotalAssets_ly * 100, 2) AS turnoverRatioOfTotalAssets_ly,
       round(turnoverRatioOfTotalAssets_l2y * 100, 2) AS turnoverRatioOfTotalAssets_l2y,
       round(turnoverRatioOfTotalAssets_l3y * 100, 2) AS turnoverRatioOfTotalAssets_l3y,
       round(turnoverRatioOfTotalAssets_l4y * 100, 2) AS turnoverRatioOfTotalAssets_l4y,
       
       round(turnoverRatioOfReceivable * 100, 2) AS turnoverRatioOfReceivable,
       round(turnoverRatioOfReceivable_ly * 100, 2) AS turnoverRatioOfReceivable_ly,
       round(turnoverRatioOfReceivable_l2y * 100, 2) AS turnoverRatioOfReceivable_l2y,
       round(turnoverRatioOfReceivable_l3y * 100, 2) AS turnoverRatioOfReceivable_l3y,
       round(turnoverRatioOfReceivable_l4y * 100, 2) AS turnoverRatioOfReceivable_l4y,
       
       round(cashOfnetProfit_TTM * 100, 2) AS cashOfnetProfit_TTM,
       round(cashOfnetProfit_TTM_ly * 100, 2) AS cashOfnetProfit_TTM_ly,
       round(cashOfnetProfit_TTM_l2y * 100, 2) AS cashOfnetProfit_TTM_l2y,
       round(cashOfnetProfit_TTM_l3y * 100, 2) AS cashOfnetProfit_TTM_l3y,
       round(cashOfnetProfit_TTM_l4y * 100, 2) AS cashOfnetProfit_TTM_l4y,
       
       round(cashOfnetProfit * 100, 2) AS cashOfnetProfit,
       round(cashOfnetProfit_ly * 100, 2) AS cashOfnetProfit_ly,
       round(cashOfnetProfit_l2y * 100, 2) AS cashOfnetProfit_l2y,
       round(cashOfnetProfit_l3y * 100, 2) AS cashOfnetProfit_l3y,
       round(cashOfnetProfit_l4y * 100, 2) AS cashOfnetProfit_l4y,
       
       round(case
               when operatingRevenue_TTM_ly = 0 then
                0
               else
                operatingRevenue_TTM / operatingRevenue_TTM_ly - 1
             end * 100,
             2) AS operatingRinrate,
       round(case
               when operatingRevenue_TTM_l2y = 0 then
                0
               else
                operatingRevenue_TTM_ly / operatingRevenue_TTM_l2y - 1
             end * 100,
             2) AS operatingRinrate_ly,
       round(case
               when operatingRevenue_TTM_l3y = 0 then
                0
               else
                operatingRevenue_TTM_l2y / operatingRevenue_TTM_l3y - 1
             end * 100,
             2) AS operatingRinrate_l2y,
       round(case
               when operatingRevenue_TTM_l4y = 0 then
                0
               else
                operatingRevenue_TTM_l3y / operatingRevenue_TTM_l4y - 1
             end * 100,
             2) AS operatingRinrate_l3y,
       round(case
               when netProfit_TTM_ly = 0 then
                0
               else
                netProfit_TTM / netProfit_TTM_ly - 1
             end * 100,
             2) as netProfit_inrate,
       round(case
               when netProfit_TTM_l2y = 0 then
                0
               else
                netProfit_TTM_ly / netProfit_TTM_l2y - 1
             end * 100,
             2) as netProfit_inrate_ly,
       round(case
               when netProfit_TTM_l3y = 0 then
                0
               else
                netProfit_TTM_l2y / netProfit_TTM_l3y - 1
             end * 100,
             2) as netProfit_inrate_l2y,
       round(case
               when netProfit_TTM_l4y = 0 then
                0
               else
                netProfit_TTM_l3y / netProfit_TTM_l4y - 1
             end * 100,
             2) as netProfit_inrate_l3y,
       round(case
               when netCashOperatActiv_TTM_ly = 0 then
                0
               else
                netCashOperatActiv_TTM / netCashOperatActiv_TTM_ly - 1
             end * 100,
             2) as netCashOperatinrate,
       round(case
               when netCashOperatActiv_TTM_l2y = 0 then
                0
               else
                netCashOperatActiv_TTM_ly / netCashOperatActiv_TTM_l2y - 1
             end * 100,
             2) as netCashOperatinrate_ly,
       round(case
               when netCashOperatActiv_TTM_l3y = 0 then
                0
               else
                netCashOperatActiv_TTM_l2y / netCashOperatActiv_TTM_l3y - 1
             end * 100,
             2) as netCashOperatinrate_l2y,
       round(case
               when netCashOperatActiv_TTM_l4y = 0 then
                0
               else
                netCashOperatActiv_TTM_l3y / netCashOperatActiv_TTM_l4y - 1
             end * 100,
             2) as netCashOperatinrate_l3y,
       round(case
               when totalProfit_TTM_ly = 0 then
                0
               else
                totalProfit_TTM / totalProfit_TTM_ly - 1
             end * 100,
             2) as totalProfitinrate,
       round(case
               when totalProfit_TTM_l2y = 0 then
                0
               else
                totalProfit_TTM_ly / totalProfit_TTM_l2y - 1
             end * 100,
             2) as totalProfitinrate_ly,
       round(case
               when totalProfit_TTM_l3y = 0 then
                0
               else
                totalProfit_TTM_l2y / totalProfit_TTM_l3y - 1
             end * 100,
             2) as totalProfitinrate_l2y,
       round(case
               when totalProfit_TTM_l4y = 0 then
                0
               else
                totalProfit_TTM_l3y / totalProfit_TTM_l4y - 1
             end * 100,
             2) as totalProfitinrate_l3y,
       round(case
               when TOTALLIABILITIES = 0 then
                0
               else
                total_market / TOTALLIABILITIES
             end * 100,
             2) as PT,
       round(case
               when MONEYFUNDS = 0 then
                0
               else
                total_market / MONEYFUNDS
             end * 100,
             2) as PM,
       round(case
               when OPERATINGREVENUE_TTM = 0 then
                0
               else
                total_market / OPERATINGREVENUE_TTM
             end * 100,
             2) as PS,
       round(case
               when NETCASHOPERATACTIV_TTM = 0 then
                0
               else
                total_market / NETCASHOPERATACTIV_TTM
             end * 100,
             2) as PC,
       round(case
               when NETPROFIT_TTM_LY = 0 or NETPROFIT_TTM = NETPROFIT_TTM_LY then
                0
               else
                pe / (NETPROFIT_TTM / NETPROFIT_TTM_LY - 1) / 100
             end * 100,
             2) as PEG,
       round(case
               when OPERATINGREVENUE_TTM_LY = 0 or
                    OPERATINGREVENUE_TTM = OPERATINGREVENUE_TTM_LY then
                0
               else
                PE / (OPERATINGREVENUE_TTM / OPERATINGREVENUE_TTM_LY - 1) / 100
             end * 100,
             2) as PSG,
       round(case
               when totalassets_LY = 0 or totalassets = totalassets_LY then
                0
               else
                PE / (totalassets / totalassets_LY - 1) / 100
             end * 100,
             2) as PBG,
       round(AVG5_TOR * 100, 2) as AVG5_TOR,
       round(AVG20_TOR * 100, 2) as AVG20_TOR,
       round(AVG30_TOR * 100, 2) as AVG30_TOR,
       round(AVG60_TOR * 100, 2) as AVG60_TOR,
       round(RNG_L * 100, 2) as RNG_L,
       round(RNG_5 * 100, 2) as RNG_5,
       round(RNG_20 * 100, 2) as RNG_20,
       round(RNG_30 * 100, 2) as RNG_30,
       round(RNG_60 * 100, 2) as RNG_60,
       round((case
               when LAG_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                TOTAL_market / LAG_MARKET - 1
             end) * 100,
             2) as lag,
       round((case
               when LAG2_MARKET = 0 or LAG2_MARKET is null then
                0
               else
                TOTAL_market / LAG2_MARKET - 1
             end) * 100,
             2) as lag2,
       round((case
               when LAG3_MARKET = 0 or LAG3_MARKET is null then
                0
               else
                TOTAL_market / LAG3_MARKET - 1
             end) * 100,
             2) as lag3,
       round((case
               when LAG5_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                TOTAL_market / LAG5_MARKET - 1
             end) * 100,
             2) as lag5,
       round((case
               when LAG20_MARKET = 0 or LAG20_MARKET is null then
                0
               else
                TOTAL_market / LAG20_MARKET - 1
             end) * 100,
             2) as lag20,
       round((case
               when LAG30_MARKET = 0 or LAG30_MARKET is null then
                0
               else
                TOTAL_market / LAG30_MARKET - 1
             end) * 100,
             2) as lag30,
       round((case
               when LAG60_MARKET = 0 or LAG60_MARKET is null then
                0
               else
                TOTAL_market / LAG60_MARKET - 1
             end) * 100,
             2) as lag60,
       round((case
               when LAG_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                TOTAL_market / LAG_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG_ALL_MARKET = 0 or LAG_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag,
       round((case
               when LAG2_MARKET = 0 or LAG2_MARKET is null then
                0
               else
                TOTAL_market / LAG2_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG2_ALL_MARKET = 0 or LAG2_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG2_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag2,
       round((case
               when LAG3_MARKET = 0 or LAG3_MARKET is null then
                0
               else
                TOTAL_market / LAG3_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG3_ALL_MARKET = 0 or LAG3_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG3_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag3,
       round((case
               when LAG5_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                TOTAL_market / LAG5_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG5_ALL_MARKET = 0 or LAG5_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG5_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag5,
       round((case
               when LAG20_MARKET = 0 or LAG20_MARKET is null then
                0
               else
                TOTAL_market / LAG20_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG20_ALL_MARKET = 0 or LAG20_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG20_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag20,
       round((case
               when LAG30_MARKET = 0 or LAG30_MARKET is null then
                0
               else
                TOTAL_market / LAG30_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG30_ALL_MARKET = 0 or LAG30_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG30_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag30,
       round((case
               when LAG60_MARKET = 0 or LAG60_MARKET is null then
                0
               else
                TOTAL_market / LAG60_MARKET - 1
             end) * 100,
             2) - round((case
                          when LAG60_ALL_MARKET = 0 or LAG60_ALL_MARKET is null then
                           0
                          else
                           all_TOTAL_MARKET / LAG60_ALL_MARKET - 1
                        end) * 100,
                        2) as diff_lag60,
       round((case
               when AVG5_A_MARKET = 0 or AVG5_A_MARKET is null then
                0
               else
                AVG_TOTAL_MARKET / AVG5_A_MARKET - 1
             end) * 100,
             2) as avg5,
       round((case
               when AVG20_A_MARKET = 0 or AVG20_A_MARKET is null then
                0
               else
                AVG_TOTAL_MARKET / AVG20_A_MARKET - 1
             end) * 100,
             2) as avg20,
       round((case
               when AVG30_A_MARKET = 0 or AVG30_A_MARKET is null then
                0
               else
                AVG_TOTAL_MARKET / AVG30_A_MARKET - 1
             end) * 100,
             2) as avg30,
       round((case
               when AVG60_A_MARKET = 0 or AVG60_A_MARKET is null then
                0
               else
                AVG_TOTAL_MARKET / AVG60_A_MARKET - 1
             end) * 100,
             2) as avg60,
       
       round((case
               when PRE_MARKET = 0 then
                0
               else
                PRE2_MARKET / PRE_MARKET - 1
             end) * 100,
             2) as target,
       round((case
               when PRE_MARKET = 0 then
                0
               else
                PRE3_MARKET / PRE_MARKET - 1
             end) * 100,
             2) as target3,
       round((case
               when PRE_MARKET = 0 then
                0
               else
                PRE5_MARKET / PRE_MARKET - 1
             end) * 100,
             2) as target5,
       round((case
               when CLOSE_QFQ = 0 then
                0
               else
                AVG_PRE_MARKET / CLOSE_QFQ - 1
             end) * 100,
             2) as avg_target
  from (select * from QUANT_ANALYSIS_DATA 
 where order_date = to_date('{start_date}', 'yyyy-mm-dd'))A
 left join (select CODE,
                    COUNT(DISTINCT decode(blockname, '上证50', 1, 0)) as SZ50,
                    COUNT(DISTINCT decode(blockname, '沪深300', 1, 0)) as HS300,
                    COUNT(DISTINCT decode(blockname, '创业300', 1, 0)) as CY300,
                    COUNT(DISTINCT decode(blockname, '上证180', 1, 0)) as SZ180,
                    COUNT(DISTINCT decode(blockname, '上证380', 1, 0)) as SZ380,
                    COUNT(DISTINCT decode(blockname, '深证100', 1, 0)) as SZ100,
                    COUNT(DISTINCT decode(blockname, '深证300', 1, 0)) as SZ300,
                    COUNT(DISTINCT decode(blockname, '中证100', 1, 0)) as ZZ100,
                    COUNT(DISTINCT decode(blockname, '中证200', 1, 0)) as ZZ200,
                    COUNT(DISTINCT decode(blockname, '创业板50', 1, 0)) as CY50
               from stock_block
              GROUP BY CODE) b
    on a.code = b.code'''
    if deal_date is None:
        print('Must Have A DATE ')
    else:
        if QA_util_if_trade(QA_util_get_pre_trade_date(deal_date,5)) == True:
            print(QA_util_get_pre_trade_date(deal_date,5))
            sql = sql.format(start_date=QA_util_get_pre_trade_date(deal_date,5))
            conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
            data = pd.read_sql(sql=sql, con=conn)
            conn.close()
        else:
            data = None
        if data.shape[0] == 0 or data is None:
            print("No data For {start_date}".format(start_date=deal_date))
            return None
        else:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data.drop_duplicates((['CODE', 'date_stamp'])))