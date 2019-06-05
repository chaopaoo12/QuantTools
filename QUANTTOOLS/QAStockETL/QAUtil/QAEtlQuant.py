import cx_Oracle
import pandas as pd
import datetime

from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)

def QA_util_etl_stock_quant(deal_date = None):

    sql = '''select a.code,
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
                CLOSE_QFQ / LAG_MARKET - 1
             end) * 100,
             2) as lag,
       round((case
               when LAG2_MARKET = 0 or LAG2_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG2_MARKET - 1
             end) * 100,
             2) as lag2,
       round((case
               when LAG3_MARKET = 0 or LAG3_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG3_MARKET - 1
             end) * 100,
             2) as lag3,
       round((case
               when LAG5_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG5_MARKET - 1
             end) * 100,
             2) as lag5,
       round((case
               when LAG20_MARKET = 0 or LAG20_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG20_MARKET - 1
             end) * 100,
             2) as lag20,
       round((case
               when LAG30_MARKET = 0 or LAG30_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG30_MARKET - 1
             end) * 100,
             2) as lag30,
       round((case
               when LAG60_MARKET = 0 or LAG60_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG60_MARKET - 1
             end) * 100,
             2) as lag60,
       round((case
               when AVG5_A_MARKET = 0 or AVG5_A_MARKET is null then
                0
               else
                avgrage_qfq / AVG5_A_MARKET - 1
             end) * 100,
             2) as avg5,
       round((case
               when AVG20_A_MARKET = 0 or AVG20_A_MARKET is null then
                0
               else
                avgrage_qfq / AVG20_A_MARKET - 1
             end) * 100,
             2) as avg20,
       round((case
               when AVG30_A_MARKET = 0 or AVG30_A_MARKET is null then
                0
               else
                avgrage_qfq / AVG30_A_MARKET - 1
             end) * 100,
             2) as avg30,
       round((case
               when AVG60_A_MARKET = 0 or AVG60_A_MARKET is null then
                0
               else
                avgrage_qfq / AVG60_A_MARKET - 1
             end) * 100,
             2) as avg60
  from (select *
          from stock_analysis_data
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
        if QA_util_if_trade(deal_date) == True:
            print(deal_date)
            sql = sql.format(start_date=deal_date)
            conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
            data = pd.read_sql(sql=sql, con=conn)
            conn.close()
        else:
            data = None
        if data is None:
            print("No data For {start_date}".format(start_date=deal_date))
            return None
        else:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data.drop_duplicates((['CODE', 'date_stamp'])))