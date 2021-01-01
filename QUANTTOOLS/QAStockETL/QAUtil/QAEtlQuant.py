import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)


def QA_util_etl_stock_quant(deal_date = None,ui_log= None):
    QA_util_log_info(
    '##JOB01 Now Etl Stock QuantData ==== {}'.format(deal_date), ui_log)
    sql = '''select a.code,
       name,
       industry,
       to_char(order_date, 'yyyy-mm-dd') as "date",
       order_Date - market_day as days,
       total_market,
       round(tra_total_market / total_market * 100, 2) AS tra_rate,
       round(pe_ttm, 2) AS pe_ttm,
       round(i_PE, 2) AS i_pe,
       round(peegl_ttm, 2) AS peegl_ttm,
       round(i_PEEGL, 2) AS i_peegl,
       round(pb, 2) AS pb,
       round(i_PB, 2) AS i_pb,
       round(roe * 100, 2) AS roe,
       round(roe_ttm * 100, 2) AS roe_ttm,
       round(netroe * 100, 2) AS netroe,
       round(netroe_ttm * 100, 2) AS netroe_ttm,
       round(case
               when i_PE = 0 then
                0
               else
                PE_TTM / i_PE
             end,
             2) as pe_rate,
       round(case
               when i_PE = 0 then
                0
               else
                peegl_ttm / i_PE
             end,
             2) as peegl_rate,
       round(case
               when i_PB = 0 then
                0
               else
                pb / i_PB
             end,
             2) as pb_rate,
       round(case
               when i_ROE = 0 then
                0
               else
                roe / i_ROE
             end,
             2) as roe_rate,
       round(case
               when i_ROA = 0 then
                0
               else
                roa / i_ROA
             end,
             2) as roa_rate,
       round(case
               when i_grossMargin = 0 then
                0
               else
                grossMargin / i_grossMargin
             end,
             2) as gross_rate,
       round((roe_yoy + roe_l2y + roe_l3y + roe_l4y) / 5, 2) as roe_avg5,
       round((roa_yoy + roa_l2y + roa_l3y + roa_l4y) / 5, 2) as roa_avg5,
       round((grossMargin_yoy + grossMargin_l2y + grossMargin_l3y +
             grossMargin_l4y) / 5,
             2) as gross_avg5,
       round(least(roe_yoy + roe_l2y + roe_l3y + roe_l4y), 2) as roe_min,
       round(least(roa_yoy + roa_l2y + roa_l3y + roa_l4y), 2) as roa_min,
       round(least(grossMargin_yoy + grossMargin_l2y + grossMargin_l3y +
                   grossMargin_l4y),
             2) as gross_min,
       round(case
               when roe_yoy + roe_l2y + roe_l3y + roe_l4y = 0 then
                0
               else
                roe / (roe_yoy + roe_l2y + roe_l3y + roe_l4y)
             end,
             2) as roe_ch,
       round(case
               when roa_yoy + roa_l2y + roa_l3y + roa_l4y = 0 then
                0
               else
                roa / (roa_yoy + roa_l2y + roa_l3y + roa_l4y)
             end,
             2) as roa_ch,
       round(case
               when grossMargin_yoy + grossMargin_l2y + grossMargin_l3y +
                    grossMargin_l4y = 0 then
                0
               else
                grossMargin / (grossMargin_yoy + grossMargin_l2y + grossMargin_l3y +
                grossMargin_l4y)
             end,
             2) as gross_ch,
       round(i_ROE * 100, 2) AS i_roe,
       round(roe_yoy * 100, 2) AS roe_yoy,
       round(roe_l2y * 100, 2) AS roe_l2y,
       round(roe_l3y * 100, 2) AS roe_l3y,
       round(roe_l4y * 100, 2) AS roe_l4y,
       round(roa * 100, 2) AS roa,
       round(i_ROA * 100, 2) AS i_roa,
       round(roa_yoy * 100, 2) AS roa_yoy,
       round(roa_l2y * 100, 2) AS roa_l2y,
       round(roa_l3y * 100, 2) AS roa_l3y,
       round(roa_l4y * 100, 2) AS roa_l4y,
       round(grossMargin * 100, 2) AS grossMargin,
       round(i_grossMargin * 100, 2) AS i_grossMargin,
       round(grossMargin_yoy * 100, 2) AS grossMargin_yoy,
       round(grossMargin_l2y * 100, 2) AS grossMargin_l2y,
       round(grossMargin_l3y * 100, 2) AS grossMargin_l3y,
       round(grossMargin_l4y * 100, 2) AS grossMargin_l4y,
       round(assetsLiabilitiesRatio * 100, 2) AS assetsLiabilitiesRatio,
       round(assetsLiabilitiesRatio_yoy * 100, 2) AS assetsLiabilitiesRatio_yoy,
       round(assetsLiabilitiesRatio_l2y * 100, 2) AS assetsLiabilitiesRatio_l2y,
       round(assetsLiabilitiesRatio_l3y * 100, 2) AS assetsLiabilitiesRatio_l3y,
       round(assetsLiabilitiesRatio_l4y * 100, 2) AS assetsLiabilitiesRatio_l4y,
       round(cashRatio * 100, 2) AS cashRatio,
       round(cashRatio_yoy * 100, 2) AS cashRatio_yoy,
       round(cashRatio_l2y * 100, 2) AS cashRatio_l2y,
       round(cashRatio_l3y * 100, 2) AS cashRatio_l3y,
       round(cashRatio_l4y * 100, 2) AS cashRatio_l4y,
       round(tangibleAssetDebtRatio * 100, 2) AS tangibleAssetDebtRatio,
       round(tangibleAssetDebtRatio_yoy * 100, 2) AS tangibleAssetDebtRatio_yoy,
       round(tangibleAssetDebtRatio_l2y * 100, 2) AS tangibleAssetDebtRatio_l2y,
       round(tangibleAssetDebtRatio_l3y * 100, 2) AS tangibleAssetDebtRatio_l3y,
       round(tangibleAssetDebtRatio_l4y * 100, 2) AS tangibleAssetDebtRatio_l4y,
       
       round(turnoverRatioOfTotalAssets * 100, 2) AS turnoverRatioOfTotalAssets,
       round((turnoverRatioOfTotalAssets_yoy +
             turnoverRatioOfTotalAssets_l2y +
             turnoverRatioOfTotalAssets_l3y +
             turnoverRatioOfTotalAssets_l4y) / 4 * 100,
             2) AS turnoverRatioOfTotalAssets_avg,
       
       round(turnoverRatioOfReceivable * 100, 2) AS turnoverRatioOfReceivable,
       round(turnoverRatioOfReceivable_yoy * 100, 2) AS turnoverRatioOfReceivable_yoy,
       round(turnoverRatioOfReceivable_l2y * 100, 2) AS turnoverRatioOfReceivable_l2y,
       round(turnoverRatioOfReceivable_l3y * 100, 2) AS turnoverRatioOfReceivable_l3y,
       round(turnoverRatioOfReceivable_l4y * 100, 2) AS turnoverRatioOfReceivable_l4y,
       
       round(cashOfnetProfit * 100, 2) AS cashOfnetProfit,
       round(cashOfnetProfit_yoy * 100, 2) AS cashOfnetProfit_yoy,
       round(cashOfnetProfit_l2y * 100, 2) AS cashOfnetProfit_l2y,
       round(cashOfnetProfit_l3y * 100, 2) AS cashOfnetProfit_l3y,
       round(cashOfnetProfit_l4y * 100, 2) AS cashOfnetProfit_l4y,
       
       round(case
               when operatingRevenue_yoy = 0 then
                0
               else
                operatingRevenue / operatingRevenue_yoy - 1
             end * 100,
             2) AS operatingRinrate,
       round(case
               when operatingRevenue_l2y = 0 then
                0
               else
                operatingRevenue_yoy / operatingRevenue_l2y - 1
             end * 100,
             2) AS operatingRinrate_yoy,
       round(case
               when operatingRevenue_l3y = 0 then
                0
               else
                operatingRevenue_l2y / operatingRevenue_l3y - 1
             end * 100,
             2) AS operatingRinrate_l2y,
       round(case
               when operatingRevenue_l4y = 0 then
                0
               else
                operatingRevenue_l3y / operatingRevenue_l4y - 1
             end * 100,
             2) AS operatingRinrate_l3y,
       round(case
               when netProfit_TTM_yoy = 0 then
                0
               else
                netProfit_TTM / netProfit_TTM_yoy - 1
             end * 100,
             2) as netProfit_inrate,
       round(case
               when netProfit_TTM_l2y = 0 then
                0
               else
                netProfit_TTM_yoy / netProfit_TTM_l2y - 1
             end * 100,
             2) as netProfit_inrate_yoy,
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
               when netCashOperatActiv_yoy = 0 then
                0
               else
                netCashOperatActiv / netCashOperatActiv_yoy - 1
             end * 100,
             2) as netCashOperatinrate,
       round(case
               when netCashOperatActiv_l2y = 0 then
                0
               else
                netCashOperatActiv_yoy / netCashOperatActiv_l2y - 1
             end * 100,
             2) as netCashOperatinrate_yoy,
       round(case
               when netCashOperatActiv_l3y = 0 then
                0
               else
                netCashOperatActiv_l2y / netCashOperatActiv_l3y - 1
             end * 100,
             2) as netCashOperatinrate_l2y,
       round(case
               when netCashOperatActiv_l4y = 0 then
                0
               else
                netCashOperatActiv_l3y / netCashOperatActiv_l4y - 1
             end * 100,
             2) as netCashOperatinrate_l3y,
       round(case
               when totalProfit_yoy = 0 then
                0
               else
                totalProfit / totalProfit_yoy - 1
             end * 100,
             2) as totalProfitinrate,
       round(case
               when totalProfit_l2y = 0 then
                0
               else
                totalProfit_yoy / totalProfit_l2y - 1
             end * 100,
             2) as totalProfitinrate_yoy,
       round(case
               when totalProfit_l3y = 0 then
                0
               else
                totalProfit_l2y / totalProfit_l3y - 1
             end * 100,
             2) as totalProfitinrate_l2y,
       round(case
               when totalProfit_l4y = 0 then
                0
               else
                totalProfit_l3y / totalProfit_l4y - 1
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
               when OPERATINGREVENUE <= 0 then
                0
               else
                total_market / OPERATINGREVENUE
             end * 100,
             2) as PS,
       round(case
               when NETCASHOPERATACTIV <= 0 then
                0
               else
                total_market / NETCASHOPERATACTIV
             end * 100,
             2) as PC,
       round(case
               when NETPROFIT_TTM_yoy <= 0 or NETPROFIT_TTM = NETPROFIT_TTM_yoy then
                0
               else
                pe_ttm / (NETPROFIT_TTM / NETPROFIT_TTM_yoy - 1) / 100
             end * 100,
             2) as PEG,
       round(case
               when OPERATINGREVENUE_yoy <= 0 or
                    OPERATINGREVENUE = OPERATINGREVENUE_yoy then
                0
               else
                pe_ttm / (OPERATINGREVENUE / OPERATINGREVENUE_yoy - 1) / 100
             end * 100,
             2) as PSG,
       round(case
               when totalassets_yoy = 0 or totalassets = totalassets_yoy then
                0
               else
                pe_ttm / (totalassets / totalassets_yoy - 1) / 100
             end * 100,
             2) as PBG,
       round(LAG_TOR * 100, 2) as LAG_TOR,
       round(AVG5_TOR * 100, 2) as AVG5_TOR,
       round(AVG10_TOR * 100, 2) as AVG10_TOR,
       round(AVG20_TOR * 100, 2) as AVG20_TOR,
       round(AVG30_TOR * 100, 2) as AVG30_TOR,
       round(AVG60_TOR * 100, 2) as AVG60_TOR,
       RNG,
       to_number(RNG_L) as RNG_L,
       to_number(RNG_5) as RNG_5,
       to_number(RNG_10) as RNG_10,
       to_number(RNG_20) as RNG_20,
       to_number(RNG_30) as RNG_30,
       to_number(RNG_60) as RNG_60,
       to_number(RNG_90) as RNG_90,
       AVG5_RNG,
       AVG10_RNG,
       AVG20_RNG,
       AVG30_RNG,
       AVG60_RNG,
       round((case
               when LAG_MARKET = 0 or LAG_MARKET is null then
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
               when LAG10_MARKET = 0 or LAG10_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG10_MARKET - 1
             end) * 100,
             2) as lag10,
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
               when LAG90_MARKET = 0 or LAG90_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG90_MARKET - 1
             end) * 100,
             2) as lag90,
       round((case
               when AVG5_A_MARKET = 0 or AVG5_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG5_A_MARKET - 1
             end) * 100,
             2) as avg5,
       round((case
               when AVG10_A_MARKET = 0 or AVG10_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG10_A_MARKET - 1
             end) * 100,
             2) as avg10,
       round((case
               when AVG20_A_MARKET = 0 or AVG20_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG20_A_MARKET - 1
             end) * 100,
             2) as avg20,
       round((case
               when AVG30_A_MARKET = 0 or AVG30_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG30_A_MARKET - 1
             end) * 100,
             2) as avg30,
       round((case
               when AVG60_A_MARKET = 0 or AVG60_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG60_A_MARKET - 1
             end) * 100,
             2) as avg60,
       round((case
               when AVG90_A_MARKET = 0 or AVG90_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG90_A_MARKET - 1
             end) * 100,
             2) as avg90,
       AMT_L,
       AMT_5,
       AMT_10,
       AMT_20,
       AMT_30,
       AMT_60,
       AMT_90,
       MAMT_5,
       MAMT_10,
       MAMT_20,
       MAMT_30,
       MAMT_60,
       MAMT_90,
       AVG5_CR,
       AVG10_CR,
       AVG20_CR,
       AVG30_CR,
       AVG60_CR,
       AVG90_CR,
       AVG5_TR,
       AVG10_TR,
       AVG20_TR,
       AVG30_TR,
       AVG60_TR,
       AVG90_TR,
       case
         when POSRT_CNT5 + NEGRT_CNT5 = 0 then
          0
         else
          NEGRT_CNT5 / (POSRT_CNT5 + NEGRT_CNT5)
       end as neg5_rate,
       case
         when POSRT_MEAN5 = 0 or POSRT_MEAN5 is null then
          0
         else
          abs(NEGRT_MEAN5) / POSRT_MEAN5
       end as neg5_rt,
       NEGRT_CNT10 / (POSRT_CNT10 + NEGRT_CNT10) as neg10_rate,
       case
         when POSRT_MEAN10 = 0 or POSRT_MEAN10 is null then
          0
         else
          abs(NEGRT_MEAN10) / POSRT_MEAN10
       end as neg10_rt,
       NEGRT_CNT20 / (POSRT_CNT20 + NEGRT_CNT20) as neg20_rate,
       case
         when POSRT_MEAN20 = 0 or POSRT_MEAN20 is null then
          0
         else
          abs(NEGRT_MEAN20) / POSRT_MEAN20
       end as neg20_rt,
       NEGRT_CNT30 / (POSRT_CNT30 + NEGRT_CNT30) as neg30_rate,
       case
         when POSRT_MEAN30 = 0 or POSRT_MEAN30 is null then
          0
         else
          abs(NEGRT_MEAN30) / POSRT_MEAN30
       end as neg30_rt,
       NEGRT_CNT60 / (POSRT_CNT60 + NEGRT_CNT60) as neg60_rate,
       case
         when POSRT_MEAN60 = 0 or POSRT_MEAN60 is null then
          0
         else
          abs(NEGRT_MEAN60) / POSRT_MEAN60
       end as neg60_rt,
       NEGRT_CNT90 / (POSRT_CNT90 + NEGRT_CNT90) as neg90_rate,
       case
         when POSRT_MEAN90 = 0 or POSRT_MEAN90 is null then
          0
         else
          abs(NEGRT_MEAN90) / POSRT_MEAN90
       end as neg90_rt,
       to_number(avg5_c_market) as avg5_c_market,
       to_number(avg10_c_market) as avg10_c_market,
       to_number(avg20_c_market) as avg20_c_market,
       to_number(avg30_c_market) as avg30_c_market,
       to_number(avg60_c_market) as avg60_c_market,
       to_number(avg90_c_market) as avg90_c_market,
       pe_rank,
       pb_rank
  from stock_analysis_data a
 where order_date = to_date('{start_date}', 'yyyy-mm-dd')
 and (turnoverRatio * 1000 >= 10 or order_Date - market_day >= 15)
'''
    if deal_date is None:
        QA_util_log_info('Must Have A DATE')
    else:
        if QA_util_if_trade(deal_date) == True:
            sql = sql.format(start_date=deal_date)
            conn = cx_Oracle.connect(ORACLE_PATH2)
            data = pd.read_sql(sql=sql, con=conn)
            conn.close()
        else:
            data = None
        if data is None:
            QA_util_log_info("No data For {start_date}".format(start_date=deal_date))
            return None
        else:
            data = data.assign(NETCASHOPERATINRATE_AVG3 = (data.NETCASHOPERATINRATE_YOY + data.NETCASHOPERATINRATE_L2Y + data.NETCASHOPERATINRATE_L3Y)/3)
            data = data.assign(NETPRTAX_RATE = data.NETPROFIT_INRATE / data.TOTALPROFITINRATE)
            data = data.assign(OPINRATE_AVG3 = (data.OPERATINGRINRATE_YOY + data.OPERATINGRINRATE_L2Y + data.OPERATINGRINRATE_L3Y)/3)
            data = data.assign(NETPINRATE_AVG3 = (data.NETPROFIT_INRATE_YOY + data.NETPROFIT_INRATE_L2Y + data.NETPROFIT_INRATE_L3Y)/3)
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data.drop_duplicates((['CODE', 'date_stamp'])))