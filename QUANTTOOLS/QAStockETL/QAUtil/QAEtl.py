import cx_Oracle
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,QA_util_log_info,
                               QA_util_get_trade_range,QA_util_get_last_day,
                               QA_util_if_trade)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

def QA_util_process_financial(deal_date = None, type = 'day'):

    if type == 'day' and deal_date == None:
        deal_date = QA_util_today_str()
    elif type == 'all':
        QA_util_log_info("Run This JOB in DataBase")
    if QA_util_if_trade(deal_date) == True:
        sql3="""insert into stock_analysis_data
  select
  /*+ append parallel(b, 16) nologging */
   g.*,
   avg(PE_TTM) over(partition by order_date, industry) as i_PE,
   avg(PEEGL_TTM) over(partition by order_date, industry) as i_PEEGL,
   avg(PB) over(partition by order_date, industry) as i_PB,
   avg(grossMargin) over(partition by order_date, industry) as i_grossMargin,
   avg(ROE) over(partition by order_date, industry) as i_ROE,
   avg(ROA) over(partition by order_date, industry) as i_ROA,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS AVG10_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_AMOUNT,
   percent_rank() over(partition by order_date, industry order by PE_TTM) as pe_rank,
   percent_rank() over(partition by order_date, industry order by pb) as pb_rank,
   percent_rank() over(partition by order_date, industry order by PE_TTM) as i_pe_rank,
   percent_rank() over(partition by order_date, industry order by pb) as i_pb_rank
    from (select h.*,
                 sum(total_market) over(partition by order_date, industry) as i_total_market,
                 sum(netProAftExtrGainLoss_TTM) over(partition by order_date, industry) as i_netProAftExtrGainLoss_TTM,
                 sum(avgTotalAssets) over(partition by order_date, industry) as i_avgTotalAssets,
                 sum(avgGoodwill) over(partition by order_date, industry) as i_avgGoodwill,
                 sum(avgTotalLiabilities) over(partition by order_date, industry) as i_avgTotalLiabilities,
                 LAG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) as LAG_TOR,
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_TOR,
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS AVG10_TOR,
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_TOR,
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_TOR,
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_TOR,
                 AVG(rng) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_RNG,
                 AVG(rng) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS AVG10_RNG,
                 AVG(rng) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_RNG,
                 AVG(rng) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_RNG,
                 AVG(rng) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_RNG,
                 case
                   when LAG(avg5_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg5_c_market / LAG(avg5_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg5_c_market < 0 then
                    -1
                   when avg5_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG5_CR,
                 case
                   when LAG(avg10_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg10_c_market / LAG(avg10_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg10_c_market < 0 then
                    -1
                   when avg10_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG10_CR,
                 case
                   when LAG(avg20_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg20_c_market / LAG(avg20_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg20_c_market < 0 then
                    -1
                   when avg20_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG20_CR,
                 case
                   when LAG(avg30_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg30_c_market / LAG(avg30_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg30_c_market < 0 then
                    -1
                   when avg30_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG30_CR,
                 case
                   when LAG(avg60_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg60_c_market / LAG(avg60_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg60_c_market < 0 then
                    -1
                   when avg60_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG60_CR,
                 case
                   when LAG(avg90_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg90_c_market / LAG(avg90_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) >= 0 then
                    0
                   when avg90_c_market < 0 then
                    -1
                   when avg90_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG90_CR,
                 case
                   when LAG(avg5_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg5_c_market / LAG(avg5_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg5_c_market < 0 then
                    -1
                   when avg5_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG5_TR,
                 case
                   when LAG(avg10_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg10_c_market / LAG(avg10_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg10_c_market < 0 then
                    -1
                   when avg10_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG10_TR,
                 case
                   when LAG(avg20_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg20_c_market / LAG(avg20_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg20_c_market < 0 then
                    -1
                   when avg20_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG20_TR,
                 case
                   when LAG(avg30_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg30_c_market / LAG(avg30_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg30_c_market < 0 then
                    -1
                   when avg30_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG30_TR,
                 case
                   when LAG(avg60_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg60_c_market / LAG(avg60_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg60_c_market < 0 then
                    -1
                   when avg60_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG60_TR,
                 case
                   when LAG(avg90_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) = 0 then
                    0
                   when avg90_c_market / LAG(avg90_c_market)
                    OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) <= 0 then
                    0
                   when avg90_c_market < 0 then
                    -1
                   when avg90_c_market > 0 then
                    1
                   else
                    0
                 end AS AVG90_TR,
                 sum(amount) over(partition by order_Date) as all_amount
            from (select a.order_date,
                         a.code,
                         report_date,
                         to_date(IPO, 'yyyymmdd') as market_day,
                         lastyear,
                         last2year,
                         last3year,
                         last4year,
                         last5year,
                         lag1,
                         send_date,
                         c.end_date,
                         industry,
                         name,
                         area,
                         a.OPEN,
                         a.HIGH,
                         a.LOW,
                         a.CLOSE,
                         a.VOLUME,
                         a.AMOUNT,
                         a.OPEN_QFQ,
                         a.HIGH_QFQ,
                         a.LOW_QFQ,
                         a.CLOSE_QFQ,
                         a.avg_total_market,
                         a.lag_market,
                         a.lag2_market,
                         a.lag3_market,
                         a.lag5_market,
                         a.lag10_market,
                         a.lag20_market,
                         a.lag30_market,
                         a.lag60_market,
                         a.lag90_market,
                         a.avg_lag_market,
                         a.avg_lag2_market,
                         a.avg_lag3_market,
                         a.avg_lag5_market,
                         a.avg_lag10_market,
                         a.avg_lag20_market,
                         a.avg_lag30_market,
                         a.avg_lag60_market,
                         a.avg_lag90_market,
                         a.avg5_t_market,
                         a.avg10_t_market,
                         a.avg20_t_market,
                         a.avg30_t_market,
                         a.avg60_t_market,
                         a.avg90_t_market,
                         a.avg5_a_market,
                         a.avg10_a_market,
                         a.avg20_a_market,
                         a.avg30_a_market,
                         a.avg60_a_market,
                         a.avg90_a_market,
                         a.avg5_c_market,
                         a.avg10_c_market,
                         a.avg20_c_market,
                         a.avg30_c_market,
                         a.avg60_c_market,
                         a.avg90_c_market,
                         LAG_HIGH,
                         HIGH_5,
                         HIGH_10,
                         HIGH_20,
                         HIGH_30,
                         HIGH_60,
                         HIGH_90,
                         LAG_LOW,
                         LOW_5,
                         LOW_10,
                         LOW_20,
                         LOW_30,
                         LOW_60,
                         LOW_90,
                         a.high / a.low - 1 as rng,
                         a.rng_l,
                         a.rng_5,
                         a.rng_10,
                         a.rng_20,
                         a.rng_30,
                         a.rng_60,
                         a.rng_90,
                         a.amt_l,
                         a.amt_5,
                         a.amt_10,
                         a.amt_20,
                         a.amt_30,
                         a.amt_60,
                         a.amt_90,
                         a.mamt_5,
                         a.mamt_10,
                         a.mamt_20,
                         a.mamt_30,
                         a.mamt_60,
                         a.mamt_90,
                         a.NEGRT_CNT5 as NEGRT_CNT5,
                         a.POSRT_CNT5 as POSRT_CNT5,
                         a.NEGRT_MEAN5,
                         a.POSRT_MEAN5,
                         a.NEGRT_CNT10,
                         a.POSRT_CNT10,
                         a.NEGRT_MEAN10,
                         a.POSRT_MEAN10,
                         a.NEGRT_CNT20,
                         a.POSRT_CNT20,
                         a.NEGRT_MEAN20,
                         a.POSRT_MEAN20,
                         a.NEGRT_CNT30,
                         a.POSRT_CNT30,
                         a.NEGRT_MEAN30,
                         a.POSRT_MEAN30,
                         a.NEGRT_CNT60,
                         a.POSRT_CNT60,
                         a.NEGRT_MEAN60,
                         a.POSRT_MEAN60,
                         a.NEGRT_CNT90,
                         a.POSRT_CNT90,
                         a.NEGRT_MEAN90,
                         a.POSRT_MEAN90,
                         b.shares_after * 10000 as shares,
                         DECODE(b.shares_after * 10000,
                                0,
                                0,
                                a.volume / b.shares_after / 100) as turnoverRatio,
                         round(a.close * b.shares_after * 10000, 2) AS total_market,
                         round(a.open * b.shares_after * 10000, 2) AS open_market,
                         round(a.high * b.shares_after * 10000, 2) AS high_market,
                         round(a.low * b.shares_after * 10000, 2) AS low_market,
                         round(a.close * b.tra_ashares_after * 10000, 2) AS tra_total_market,
                         case
                           when netProfit_TTM = 0 then
                            0
                           else
                            round(a.close * b.shares_after * 10000, 2) /
                            netProfit_TTM
                         end as PE_TTM,
                         case
                           when netProAftExtrGainLoss_TTM = 0 then
                            0
                           else
                            round(a.close * b.shares_after * 10000, 2) /
                            netProAftExtrGainLoss_TTM
                         end as PEEGL_TTM,
                         case
                           when totalAssets - goodwill - totalLiabilities <= 0 then
                            0
                           else
                            round(a.close * b.shares_after * 10000, 2) /
                            (totalAssets - goodwill - totalLiabilities)
                         end as PB,
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
                         operatingRevenue,
                         operatingCosts,
                         taxAndSurcharges,
                         salesCosts,
                         managementCosts,
                         explorationCosts,
                         financialCosts,
                         operatingProfit,
                         totalProfit,
                         incomeTax,
                         NETPROFIT,
                         NETPROFIT_TTM,
                         NETPROAFTEXTRGAINLOSS,
                         NETPROAFTEXTRGAINLOSS_TTM,
                         ROA,
                         ROA_TTM,
                         ROE,
                         ROE_TTM,
                         GROSSMARGIN,
                         NETROE,
                         NETROE_TTM,
                         NETPROFITMARGINONSALES,
                         INCOMETAXRATIO,
                         REINVESTEDINCOMERATIO,
                         DEPRECIATIONRATIO,
                         OPERATINGCASHRATIO,
                         AVGFIEXDOFASSETS,
                         FIEXDOFASSETS,
                         ACIDTESTRATIO,
                         TURNOVERRATIOOFRECEIVABLE,
                         TURNOVERRATIOOFINVENTORY,
                         TURNOVERRATIOOFTOTALASSETS,
                         DEPRECIATIONOFTOTALCOSTS,
                         CASHOFNETPROFIT,
                         CASHOFINTEREST,
                         ASSETSLIABILITIESRATIO,
                         TANGIBLEASSETDEBTRATIO,
                         CASHRATIO,
                         TOTALASSETS_YOY,
                         AVGTOTALASSETS_YOY,
                         FIXEDASSETS_YOY,
                         AVGFIXEDASSETS_YOY,
                         GOODWILL_YOY,
                         AVGGOODWILL_YOY,
                         INVENTORY_YOY,
                         MONEYFUNDS_YOY,
                         ACCOUNTSPAYABLE_YOY,
                         AVGACCOUNTSPAYABLE_YOY,
                         AVGINVENTORY_YOY,
                         TOTALLIQUIDASSETS_YOY,
                         AVGTOTALLIQUIDASSETS_YOY,
                         TOTALLIABILITIES_YOY,
                         AVGTOTALLIABILITIES_YOY,
                         ACCOUNTSRECEIVABLES_YOY,
                         AVGACCOUNTSRECEIVABLES_YOY,
                         INTERCOMPANYRECEIVABLES_YOY,
                         AVGINTERCOMPANYRECEIVABLES_YOY,
                         PREPAYMENTS_YOY,
                         AVGPREPAYMENTS_YOY,
                         TOTALCURRENTLIABILITIES_YOY,
                         AVGTOTALCURRENTLIABILITIES_YOY,
                         NETCASHOPERATACTIV_YOY,
                         CASHOUTINVESTACTIV_YOY,
                         operatingRevenue_YOY,
                         operatingCosts_YOY,
                         taxAndSurcharges_YOY,
                         salesCosts_YOY,
                         managementCosts_YOY,
                         explorationCosts_YOY,
                         financialCosts_YOY,
                         operatingProfit_YOY,
                         totalProfit_YOY,
                         incomeTax_YOY,
                         NETPROFIT_YOY,
                         NETPROFIT_TTM_YOY,
                         NETPROAFTEXTRGAINLOSS_YOY,
                         NETPROAFTEXTRGAINLOSS_TTM_YOY,
                         ROA_YOY,
                         ROA_TTM_YOY,
                         ROE_YOY,
                         ROE_TTM_YOY,
                         GROSSMARGIN_YOY,
                         NETROE_YOY,
                         NETROE_TTM_YOY,
                         NETPROFITMARGINONSALES_YOY,
                         INCOMETAXRATIO_YOY,
                         REINVESTEDINCOMERATIO_YOY,
                         DEPRECIATIONRATIO_YOY,
                         OPERATINGCASHRATIO_YOY,
                         AVGFIEXDOFASSETS_YOY,
                         FIEXDOFASSETS_YOY,
                         ACIDTESTRATIO_YOY,
                         TURNOVERRATIOOFRECEIVABLE_YOY,
                         TURNOVERRATIOOFINVENTORY_YOY,
                         TURNOVERRATIOOFTOTALASSETS_YOY,
                         DEPRECIATIONOFTOTALCOSTS_YOY,
                         CASHOFNETPROFIT_YOY,
                         CASHOFINTEREST_YOY,
                         ASSETSLIABILITIESRATIO_YOY,
                         TANGIBLEASSETDEBTRATIO_YOY,
                         CASHRATIO_YOY,
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
                         operatingRevenue_LQ,
                         operatingCosts_LQ,
                         taxAndSurcharges_LQ,
                         salesCosts_LQ,
                         managementCosts_LQ,
                         explorationCosts_LQ,
                         financialCosts_LQ,
                         operatingProfit_LQ,
                         totalProfit_LQ,
                         incomeTax_LQ,
                         NETPROFIT_LQ,
                         NETPROFIT_TTM_LQ,
                         NETPROAFTEXTRGAINLOSS_LQ,
                         NETPROAFTEXTRGAINLOSS_TTM_LQ,
                         ROA_LQ,
                         ROA_TTM_LQ,
                         ROE_LQ,
                         ROE_TTM_LQ,
                         GROSSMARGIN_LQ,
                         NETROE_LQ,
                         NETROE_TTM_LQ,
                         NETPROFITMARGINONSALES_LQ,
                         INCOMETAXRATIO_LQ,
                         REINVESTEDINCOMERATIO_LQ,
                         DEPRECIATIONRATIO_LQ,
                         OPERATINGCASHRATIO_LQ,
                         AVGFIEXDOFASSETS_LQ,
                         FIEXDOFASSETS_LQ,
                         ACIDTESTRATIO_LQ,
                         TURNOVERRATIOOFRECEIVABLE_LQ,
                         TURNOVERRATIOOFINVENTORY_LQ,
                         TURNOVERRATIOOFTOTALASSETS_LQ,
                         DEPRECIATIONOFTOTALCOSTS_LQ,
                         CASHOFNETPROFIT_LQ,
                         CASHOFINTEREST_LQ,
                         ASSETSLIABILITIESRATIO_LQ,
                         TANGIBLEASSETDEBTRATIO_LQ,
                         CASHRATIO_LQ,
                         TOTALASSETS_l2y,
                         AVGTOTALASSETS_l2y,
                         FIXEDASSETS_l2y,
                         AVGFIXEDASSETS_l2y,
                         GOODWILL_l2y,
                         AVGGOODWILL_l2y,
                         INVENTORY_l2y,
                         MONEYFUNDS_l2y,
                         ACCOUNTSPAYABLE_l2y,
                         AVGACCOUNTSPAYABLE_l2y,
                         AVGINVENTORY_l2y,
                         TOTALLIQUIDASSETS_l2y,
                         AVGTOTALLIQUIDASSETS_l2y,
                         TOTALLIABILITIES_l2y,
                         AVGTOTALLIABILITIES_l2y,
                         ACCOUNTSRECEIVABLES_l2y,
                         AVGACCOUNTSRECEIVABLES_l2y,
                         INTERCOMPANYRECEIVABLES_l2y,
                         AVGINTERCOMPANYRECEIVABLES_l2y,
                         PREPAYMENTS_l2y,
                         AVGPREPAYMENTS_l2y,
                         TOTALCURRENTLIABILITIES_l2y,
                         AVGTOTALCURRENTLIABILITIES_l2y,
                         NETCASHOPERATACTIV_l2y,
                         CASHOUTINVESTACTIV_l2y,
                         operatingRevenue_l2y,
                         operatingCosts_l2y,
                         taxAndSurcharges_l2y,
                         salesCosts_l2y,
                         managementCosts_l2y,
                         explorationCosts_l2y,
                         financialCosts_l2y,
                         operatingProfit_l2y,
                         totalProfit_l2y,
                         incomeTax_l2y,
                         NETPROFIT_l2y,
                         NETPROFIT_TTM_l2y,
                         NETPROAFTEXTRGAINLOSS_l2y,
                         NETPROAFTEXTRGAINLOSS_TTM_l2y,
                         ROA_l2y,
                         ROA_TTM_l2y,
                         ROE_l2y,
                         ROE_TTM_l2y,
                         GROSSMARGIN_l2y,
                         NETROE_l2y,
                         NETROE_TTM_l2y,
                         NETPROFITMARGINONSALES_l2y,
                         INCOMETAXRATIO_l2y,
                         REINVESTEDINCOMERATIO_l2y,
                         DEPRECIATIONRATIO_l2y,
                         OPERATINGCASHRATIO_l2y,
                         AVGFIEXDOFASSETS_l2y,
                         FIEXDOFASSETS_l2y,
                         ACIDTESTRATIO_l2y,
                         TURNOVERRATIOOFRECEIVABLE_l2y,
                         TURNOVERRATIOOFINVENTORY_l2y,
                         TURNOVERRATIOOFTOTALASSETS_l2y,
                         DEPRECIATIONOFTOTALCOSTS_l2y,
                         CASHOFNETPROFIT_l2y,
                         CASHOFINTEREST_l2y,
                         ASSETSLIABILITIESRATIO_l2y,
                         TANGIBLEASSETDEBTRATIO_l2y,
                         CASHRATIO_l2y,
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
                         operatingRevenue_l3y,
                         operatingCosts_l3y,
                         taxAndSurcharges_l3y,
                         salesCosts_l3y,
                         managementCosts_l3y,
                         explorationCosts_l3y,
                         financialCosts_l3y,
                         operatingProfit_l3y,
                         totalProfit_l3y,
                         incomeTax_l3y,
                         NETPROFIT_L3Y,
                         NETPROFIT_TTM_L3Y,
                         NETPROAFTEXTRGAINLOSS_L3Y,
                         NETPROAFTEXTRGAINLOSS_TTM_L3Y,
                         ROA_L3Y,
                         ROA_TTM_L3Y,
                         ROE_L3Y,
                         ROE_TTM_L3Y,
                         GROSSMARGIN_L3Y,
                         NETROE_L3Y,
                         NETROE_TTM_L3Y,
                         NETPROFITMARGINONSALES_L3Y,
                         INCOMETAXRATIO_L3Y,
                         REINVESTEDINCOMERATIO_L3Y,
                         DEPRECIATIONRATIO_L3Y,
                         OPERATINGCASHRATIO_L3Y,
                         AVGFIEXDOFASSETS_L3Y,
                         FIEXDOFASSETS_L3Y,
                         ACIDTESTRATIO_L3Y,
                         TURNOVERRATIOOFRECEIVABLE_L3Y,
                         TURNOVERRATIOOFINVENTORY_L3Y,
                         TURNOVERRATIOOFTOTALASSETS_L3Y,
                         DEPRECIATIONOFTOTALCOSTS_L3Y,
                         CASHOFNETPROFIT_L3Y,
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
                         operatingRevenue_l4y,
                         operatingCosts_l4y,
                         taxAndSurcharges_l4y,
                         salesCosts_l4y,
                         managementCosts_l4y,
                         explorationCosts_l4y,
                         financialCosts_l4y,
                         operatingProfit_l4y,
                         totalProfit_l4y,
                         incomeTax_l4y,
                         NETPROFIT_L4Y,
                         NETPROFIT_TTM_L4Y,
                         NETPROAFTEXTRGAINLOSS_L4Y,
                         NETPROAFTEXTRGAINLOSS_TTM_L4Y,
                         ROA_L4Y,
                         ROA_TTM_L4Y,
                         ROE_L4Y,
                         ROE_TTM_L4Y,
                         GROSSMARGIN_L4Y,
                         NETROE_L4Y,
                         NETROE_TTM_L4Y,
                         NETPROFITMARGINONSALES_L4Y,
                         INCOMETAXRATIO_L4Y,
                         REINVESTEDINCOMERATIO_L4Y,
                         DEPRECIATIONRATIO_L4Y,
                         OPERATINGCASHRATIO_L4Y,
                         AVGFIEXDOFASSETS_L4Y,
                         FIEXDOFASSETS_L4Y,
                         ACIDTESTRATIO_L4Y,
                         TURNOVERRATIOOFRECEIVABLE_L4Y,
                         TURNOVERRATIOOFINVENTORY_L4Y,
                         TURNOVERRATIOOFTOTALASSETS_L4Y,
                         DEPRECIATIONOFTOTALCOSTS_L4Y,
                         CASHOFNETPROFIT_L4Y,
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
                         operatingRevenue_l5y,
                         operatingCosts_l5y,
                         taxAndSurcharges_l5y,
                         salesCosts_l5y,
                         managementCosts_l5y,
                         explorationCosts_l5y,
                         financialCosts_l5y,
                         operatingProfit_l5y,
                         totalProfit_l5y,
                         incomeTax_l5y
                         NETPROFIT_L5Y,
                         NETPROFIT_TTM_L5Y,
                         NETPROAFTEXTRGAINLOSS_L5Y,
                         NETPROAFTEXTRGAINLOSS_TTM_L5Y,
                         ROA_L5Y,
                         ROA_TTM_L5Y,
                         ROE_L5Y,
                         ROE_TTM_L5Y,
                         GROSSMARGIN_L5Y,
                         NETROE_L5Y,
                         NETROE_TTM_L5Y,
                         NETPROFITMARGINONSALES_L5Y,
                         INCOMETAXRATIO_L5Y,
                         REINVESTEDINCOMERATIO_L5Y,
                         DEPRECIATIONRATIO_L5Y,
                         OPERATINGCASHRATIO_L5Y,
                         AVGFIEXDOFASSETS_L5Y,
                         FIEXDOFASSETS_L5Y,
                         ACIDTESTRATIO_L5Y,
                         TURNOVERRATIOOFRECEIVABLE_L5Y,
                         TURNOVERRATIOOFINVENTORY_L5Y,
                         TURNOVERRATIOOFTOTALASSETS_L5Y,
                         DEPRECIATIONOFTOTALCOSTS_L5Y,
                         CASHOFNETPROFIT_L5Y,
                         CASHOFINTEREST_L5Y,
                         ASSETSLIABILITIESRATIO_L5Y,
                         TANGIBLEASSETDEBTRATIO_L5Y,
                         CASHRATIO_L5Y
                    from (select ORDER_DATE,
                                 CODE,
                                 MAX(OPEN) as OPEN,
                                 MAX(HIGH) as HIGH,
                                 MAX(LOW) as LOW,
                                 MAX(CLOSE) as CLOSE,
                                 MAX(VOLUME) as VOLUME,
                                 MAX(AMOUNT) as AMOUNT,
                                 MAX(OPEN_QFQ) as OPEN_QFQ,
                                 MAX(HIGH_QFQ) as HIGH_QFQ,
                                 MAX(LOW_QFQ) as LOW_QFQ,
                                 MAX(CLOSE_QFQ) as CLOSE_QFQ,
                                 MAX(AVG_TOTAL_MARKET) as AVG_TOTAL_MARKET,
                                 MAX(LAG_MARKET) as LAG_MARKET,
                                 MAX(AVG_LAG_MARKET) as AVG_LAG_MARKET,
                                 MAX(LAG_HIGH) as LAG_HIGH,
                                 MAX(LAG_LOW) as LAG_LOW,
                                 MAX(LAG2_MARKET) as LAG2_MARKET,
                                 MAX(AVG_LAG2_MARKET) as AVG_LAG2_MARKET,
                                 MAX(LAG3_MARKET) as LAG3_MARKET,
                                 MAX(AVG_LAG3_MARKET) as AVG_LAG3_MARKET,
                                 MAX(LAG5_MARKET) as LAG5_MARKET,
                                 MAX(AVG_LAG5_MARKET) as AVG_LAG5_MARKET,
                                 MAX(LAG10_MARKET) as LAG10_MARKET,
                                 MAX(AVG_LAG10_MARKET) as AVG_LAG10_MARKET,
                                 MAX(LAG20_MARKET) as LAG20_MARKET,
                                 MAX(AVG_LAG20_MARKET) as AVG_LAG20_MARKET,
                                 MAX(LAG30_MARKET) as LAG30_MARKET,
                                 MAX(AVG_LAG30_MARKET) as AVG_LAG30_MARKET,
                                 MAX(LAG30_HIGH) as LAG30_HIGH,
                                 MAX(LAG30_LOW) as LAG30_LOW,
                                 MAX(LAG60_MARKET) as LAG60_MARKET,
                                 MAX(AVG_LAG60_MARKET) as AVG_LAG60_MARKET,
                                 MAX(LAG60_HIGH) as LAG60_HIGH,
                                 MAX(LAG60_LOW) as LAG60_LOW,
                                 MAX(LAG90_MARKET) as LAG90_MARKET,
                                 MAX(AVG_LAG90_MARKET) as AVG_LAG90_MARKET,
                                 MAX(LAG90_HIGH) as LAG90_HIGH,
                                 MAX(LAG90_LOW) as LAG90_LOW,
                                 MAX(AVG10_T_MARKET) as AVG10_T_MARKET,
                                 MAX(AVG10_A_MARKET) as AVG10_A_MARKET,
                                 MAX(HIGH_10) as HIGH_10,
                                 MAX(LOW_10) as LOW_10,
                                 MAX(AVG20_T_MARKET) as AVG20_T_MARKET,
                                 MAX(AVG20_A_MARKET) as AVG20_A_MARKET,
                                 MAX(HIGH_20) as HIGH_20,
                                 MAX(LOW_20) as LOW_20,
                                 MAX(AVG30_T_MARKET) as AVG30_T_MARKET,
                                 MAX(AVG30_A_MARKET) as AVG30_A_MARKET,
                                 MAX(HIGH_30) as HIGH_30,
                                 MAX(LOW_30) as LOW_30,
                                 MAX(AVG60_T_MARKET) as AVG60_T_MARKET,
                                 MAX(AVG60_A_MARKET) as AVG60_A_MARKET,
                                 MAX(HIGH_60) as HIGH_60,
                                 MAX(LOW_60) as LOW_60,
                                 MAX(AVG90_T_MARKET) as AVG90_T_MARKET,
                                 MAX(AVG90_A_MARKET) as AVG90_A_MARKET,
                                 MAX(HIGH_90) as HIGH_90,
                                 MAX(LOW_90) as LOW_90,
                                 MAX(AVG5_T_MARKET) as AVG5_T_MARKET,
                                 MAX(AVG5_A_MARKET) as AVG5_A_MARKET,
                                 MAX(HIGH_5) as HIGH_5,
                                 MAX(LOW_5) as LOW_5,
                                 MAX(AVG5_C_MARKET) as AVG5_C_MARKET,
                                 MAX(AVG10_C_MARKET) as AVG10_C_MARKET,
                                 MAX(AVG20_C_MARKET) as AVG20_C_MARKET,
                                 MAX(AVG30_C_MARKET) as AVG30_C_MARKET,
                                 MAX(AVG60_C_MARKET) as AVG60_C_MARKET,
                                 MAX(AVG90_C_MARKET) as AVG90_C_MARKET,
                                 MAX(RNG_L) as RNG_L,
                                 MAX(RNG_5) as RNG_5,
                                 MAX(RNG_10) as RNG_10,
                                 MAX(RNG_20) as RNG_20,
                                 MAX(RNG_30) as RNG_30,
                                 MAX(RNG_60) as RNG_60,
                                 MAX(RNG_90) as RNG_90,
                                 MAX(AMT_L) as AMT_L,
                                 MAX(AMT_5) as AMT_5,
                                 MAX(AMT_10) as AMT_10,
                                 MAX(AMT_20) as AMT_20,
                                 MAX(AMT_30) as AMT_30,
                                 MAX(AMT_60) as AMT_60,
                                 MAX(AMT_90) as AMT_90,
                                 MAX(MAMT_5) as MAMT_5,
                                 MAX(MAMT_10) as MAMT_10,
                                 MAX(MAMT_20) as MAMT_20,
                                 MAX(MAMT_30) as MAMT_30,
                                 MAX(MAMT_60) as MAMT_60,
                                 MAX(MAMT_90) as MAMT_90,
                                 MAX(NEGRT_CNT5) as NEGRT_CNT5,
                                 MAX(POSRT_CNT5) as POSRT_CNT5,
                                 MAX(NEGRT_MEAN5) as NEGRT_MEAN5,
                                 MAX(POSRT_MEAN5) as POSRT_MEAN5,
                                 MAX(NEGRT_CNT10) as NEGRT_CNT10,
                                 MAX(POSRT_CNT10) as POSRT_CNT10,
                                 MAX(NEGRT_MEAN10) as NEGRT_MEAN10,
                                 MAX(POSRT_MEAN10) as POSRT_MEAN10,
                                 MAX(NEGRT_CNT20) as NEGRT_CNT20,
                                 MAX(POSRT_CNT20) as POSRT_CNT20,
                                 MAX(NEGRT_MEAN20) as NEGRT_MEAN20,
                                 MAX(POSRT_MEAN20) as POSRT_MEAN20,
                                 MAX(NEGRT_CNT30) as NEGRT_CNT30,
                                 MAX(POSRT_CNT30) as POSRT_CNT30,
                                 MAX(NEGRT_MEAN30) as NEGRT_MEAN30,
                                 MAX(POSRT_MEAN30) as POSRT_MEAN30,
                                 MAX(NEGRT_CNT60) as NEGRT_CNT60,
                                 MAX(POSRT_CNT60) as POSRT_CNT60,
                                 MAX(NEGRT_MEAN60) as NEGRT_MEAN60,
                                 MAX(POSRT_MEAN60) as POSRT_MEAN60,
                                 MAX(NEGRT_CNT90) as NEGRT_CNT90,
                                 MAX(POSRT_CNT90) as POSRT_CNT90,
                                 MAX(NEGRT_MEAN90) as NEGRT_MEAN90,
                                 MAX(POSRT_MEAN90) as POSRT_MEAN90
                                from stock_market_day
                               WHERE order_date >=
                                     to_date('{deal_date}','yyyy-mm-dd') - 90
                                and  order_date <=
                                     to_date('{deal_date}','yyyy-mm-dd')
                                     group by order_date, code
                                 ) a
                        left join (select code,
                                     order_date,
                                     end_date,
                                     shares_after,
                                     coalesce(shares_before, shares_after) as shares_before,
                                     tra_ashares_after,
                                     coalesce(tra_ashares_before,
                                              tra_ashares_after) as tra_ashares_before
                                from (select code,
                                             begin_date as order_date,
                                             nvl(LAG(begin_date)
                                                 OVER(PARTITION BY CODE ORDER BY
                                                      begin_date desc),
                                                 TO_DATE(to_char(SYSDATE,
                                                                 'yyyy/mm/dd'),
                                                         'yyyy/mm/dd') + 1) AS end_date,
                                             total_shares as shares_after,
                                             LAG(total_shares) OVER(PARTITION BY CODE ORDER BY begin_date ASC) AS shares_before,
                                             tra_ashares as tra_ashares_after,
                                             LAG(tra_ashares) OVER(PARTITION BY CODE ORDER BY begin_date ASC) AS tra_ashares_before
                                        from (select code,
                                                     begin_date,
                                                     max(total_shares) as total_shares,
                                                     max(tra_ashares) as tra_ashares
                                                from stock_shares
                                               group by code, begin_date) h) g) b
                          on a.code = b.code
                         and a.order_date >= b.order_date
                         and a.order_date < b.end_date
                        left join stock_financial_analysis c
                          on a.code = c.code
                         and c.send_date < a.order_date
                         and c.end_date >= a.order_date h) g
       where order_date = to_date('{deal_date}','yyyy-mm-dd')
            """.format(deal_date=deal_date)
        conn = cx_Oracle.connect(ORACLE_PATH2)
        cursor = conn.cursor()
        if type == 'all' and deal_date == None:
            QA_util_log_info("please run this job in database")
        elif type == 'day' or deal_date != None:
            cursor.execute(sql3)
            conn.commit()
            QA_util_log_info('analysis data for {deal_date} has been stored'.format(deal_date=deal_date))
        cursor.close()
        conn.commit()
        conn.close()
