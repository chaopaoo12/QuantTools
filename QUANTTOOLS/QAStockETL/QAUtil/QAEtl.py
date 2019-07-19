import cx_Oracle
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_get_trade_range,QA_util_get_last_day,
                               QA_util_if_trade)

def QA_util_process_financial(deal_date = None, type = 'day'):

    if type == 'day' and deal_date == None:
        deal_date = QA_util_today_str()
    elif type == 'all':
        print("Run This JOB in DataBase")
    if QA_util_if_trade(deal_date) == True:
        sql3="""insert /*+ append parallel(b, 16) nologging */
into stock_analysis_data
  select /*+ parallel(b, 16) nologging */
   g.*,
   decode(i_netProAftExtrGainLoss_TTM,
          0,
          0,
          i_total_market / i_netProAftExtrGainLoss_TTM) as i_PE_total,
   decode(i_avgTotalAssets - i_avgGoodwill - i_avgTotalLiabilities,
          0,
          0,
          i_total_market /
          (i_avgTotalAssets - i_avgGoodwill - i_avgTotalLiabilities)) as i_PB_total,
   decode(i_avgTotalAssets - i_avgGoodwill - i_avgTotalLiabilities,
          0,
          0,
          i_netProAftExtrGainLoss_TTM /
          (i_avgTotalAssets - i_avgGoodwill - i_avgTotalLiabilities)) as i_ROE_total,
   decode(industryoperatingCosts_TTM,
          0,
          0,
          i_operatingRevenue_TTM / industryoperatingCosts_TTM) - 1 as i_grossMargin_total,
   decode(i_avgTotalAssets - i_avgGoodwill,
          0,
          0,
          i_netProAftExtrGainLoss_TTM / (i_avgTotalAssets - i_avgGoodwill)) as i_ROA_total,
   avg(PE) over(partition by order_date, industry) as i_PE,
   avg(PB) over(partition by order_date, industry) as i_PB,
   avg(grossMargin) over(partition by order_date, industry) as i_grossMargin,
   avg(ROE) over(partition by order_date, industry) as i_ROE,
   avg(ROA) over(partition by order_date, industry) as i_ROA,
   
   decode(all_netProAftExtrGainLoss_TTM,
          0,
          0,
          all_total_market / all_netProAftExtrGainLoss_TTM) as all_PE_total,
   decode(all_avgTotalAssets - all_avgGoodwill - all_avgTotalLiabilities,
          0,
          0,
          all_total_market /
          (all_avgTotalAssets - all_avgGoodwill - all_avgTotalLiabilities)) as all_PB_total,
   decode(all_avgTotalAssets - all_avgGoodwill - all_avgTotalLiabilities,
          0,
          0,
          all_netProAftExtrGainLoss_TTM /
          (all_avgTotalAssets - all_avgGoodwill - all_avgTotalLiabilities)) as all_ROE_total,
   decode(all_operatingCosts_TTM,
          0,
          0,
          all_operatingRevenue_TTM / all_operatingCosts_TTM) - 1 as all_grossMargin_total,
   decode(all_avgTotalAssets - all_avgGoodwill,
          0,
          0,
          all_netProAftExtrGainLoss_TTM /
          (all_avgTotalAssets - all_avgGoodwill)) as all_ROA_total,
   avg(PE) over(partition by order_date) as all_PE,
   avg(PB) over(partition by order_date) as all_PB,
   avg(grossMargin) over(partition by order_date) as all_grossMargin,
   avg(ROE) over(partition by order_date) as all_ROE,
   avg(ROA) over(partition by order_date) as all_ROA,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 9 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_AMOUNT,
   AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_AMOUNT,
   percent_rank() over(partition by order_date order by pe) as pe_rank,
   percent_rank() over(partition by order_date order by pb) as pb_rank
    from (select h.*,
                 sum(total_market) over(partition by order_date, industry) as i_total_market,
                 sum(netProAftExtrGainLoss_TTM) over(partition by order_date, industry) as i_netProAftExtrGainLoss_TTM,
                 sum(avgTotalAssets) over(partition by order_date, industry) as i_avgTotalAssets,
                 sum(avgGoodwill) over(partition by order_date, industry) as i_avgGoodwill,
                 sum(avgTotalLiabilities) over(partition by order_date, industry) as i_avgTotalLiabilities,
                 sum(operatingRevenue_TTM) over(partition by order_date, industry) as i_operatingRevenue_TTM,
                 sum(operatingCosts_TTM) over(partition by order_date, industry) as industryoperatingCosts_TTM,
                 sum(total_market) over(partition by order_date) as all_total_market,
                 sum(netProAftExtrGainLoss_TTM) over(partition by order_date) as all_netProAftExtrGainLoss_TTM,
                 sum(avgTotalAssets) over(partition by order_date) as all_avgTotalAssets,
                 sum(avgGoodwill) over(partition by order_date) as all_avgGoodwill,
                 sum(avgTotalLiabilities) over(partition by order_date) as all_avgTotalLiabilities,
                 sum(operatingRevenue_TTM) over(partition by order_date) as all_operatingRevenue_TTM,
                 sum(operatingCosts_TTM) over(partition by order_date) as all_operatingCosts_TTM,
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
            from (select a.code,
                         a.order_date,
                         report_date,
                         market_day,
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
                         a.VOLUME_QFQ,
                         a.AMOUNT_QFQ,
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
                           when (netProfit_TTM <= 0 or
                                netProAftExtrGainLoss_TTM <= 0) then
                            0
                           else
                            round(a.close * b.shares_after * 10000, 2) /
                            LEAST(netProfit_TTM, netProAftExtrGainLoss_TTM)
                         end as PE,
                         case
                           when totalAssets - goodwill - totalLiabilities <= 0 then
                            0
                           else
                            round(a.close * b.shares_after * 10000, 2) /
                            (totalAssets - goodwill - totalLiabilities)
                         end as PB,
                         totalAssets,
                         avgTotalAssets,
                         fixedAssets,
                         avgFixedAssets,
                         goodwill,
                         avgGoodwill,
                         inventory,
                         moneyFunds,
                         accountsPayable,
                         avgAccountsPayable,
                         avgInventory,
                         totalLiquidAssets,
                         avgTotalLiquidAssets,
                         totalLiabilities,
                         avgTotalLiabilities,
                         accountsReceivables,
                         avgAccountsReceivables,
                         interCompanyReceivables,
                         avgInterCompanyReceivables,
                         prepayments,
                         avgPrepayments,
                         totalCurrentLiabilities,
                         avgTotalCurrentLiabilities,
                         netCashOperatActiv,
                         cashOutInvestActiv,
                         netProfit,
                         operatingRevenue_TTM,
                         operatingCosts_TTM,
                         taxAndSurcharges_TTM,
                         salesCosts_TTM,
                         managementCosts_TTM,
                         explorationCosts_TTM,
                         financialCosts_TTM,
                         assestsDevaluation_TTM,
                         operatingProfit_TTM,
                         totalProfit_TTM,
                         incomeTax_TTM,
                         netProfit_TTM,
                         netProAftExtrGainLoss_TTM,
                         interest_TTM,
                         deprecForFixedAssets_TTM,
                         netCashOperatActiv_TTM,
                         cashOutInvestActiv_TTM,
                         ROE,
                         grossMargin,
                         ROA,
                         NetProfitMarginonSales_TTM,
                         incomeTaxRatio,
                         reinvestedIncomeRatio,
                         depreciationRatio,
                         operatingCashRatio_TTM,
                         avgFiexdOfAssets,
                         fiexdOfAssets,
                         acidTestRatio,
                         turnoverRatioOfReceivable,
                         turnoverRatioOfInventory,
                         turnoverRatioOfTotalAssets,
                         depreciationOftotalCosts,
                         cashOfnetProfit,
                         cashOfnetProfit_TTM,
                         cashOfinterest,
                         assetsLiabilitiesRatio,
                         tangibleAssetDebtRatio,
                         cashRatio,
                         totalAssets_ly,
                         avgTotalAssets_ly,
                         fixedAssets_ly,
                         avgFixedAssets_ly,
                         goodwill_ly,
                         avgGoodwill_ly,
                         inventory_ly,
                         moneyFunds_ly,
                         accountsPayable_ly,
                         avgAccountsPayable_ly,
                         avgInventory_ly,
                         totalLiquidAssets_ly,
                         avgTotalLiquidAssets_ly,
                         totalLiabilities_ly,
                         avgTotalLiabilities_ly,
                         accountsReceivables_ly,
                         avgAccountsReceivables_ly,
                         interCompanyReceivables_ly,
                         avgInterCompanyReceivables_ly,
                         prepayments_ly,
                         avgPrepayments_ly,
                         totalCurrentLiabilities_ly,
                         avgTotalCurrentLiabilities_ly,
                         netCashOperatActiv_ly,
                         cashOutInvestActiv_ly,
                         netProfit_ly,
                         operatingRevenue_TTM_ly,
                         operatingCosts_TTM_ly,
                         taxAndSurcharges_TTM_ly,
                         salesCosts_TTM_ly,
                         managementCosts_TTM_ly,
                         explorationCosts_TTM_ly,
                         financialCosts_TTM_ly,
                         assestsDevaluation_TTM_ly,
                         operatingProfit_TTM_ly,
                         totalProfit_TTM_ly,
                         incomeTax_TTM_ly,
                         netProfit_TTM_ly,
                         netProAftExtrGainLoss_TTM_ly,
                         interest_TTM_ly,
                         deprecForFixedAssets_TTM_ly,
                         netCashOperatActiv_TTM_ly,
                         cashOutInvestActiv_TTM_ly,
                         ROE_ly,
                         grossMargin_ly,
                         ROA_ly,
                         NetProfitMarginonSales_TTM_ly,
                         incomeTaxRatio_ly,
                         reinvestedIncomeRatio_ly,
                         depreciationRatio_ly,
                         operatingCashRatio_TTM_ly,
                         avgFiexdOfAssets_ly,
                         fiexdOfAssets_ly,
                         acidTestRatio_ly,
                         turnoverRatioOfReceivable_ly,
                         turnoverRatioOfInventory_ly,
                         turnoverRatioOfTotalAssets_ly,
                         depreciationOftotalCosts_ly,
                         cashOfnetProfit_ly,
                         cashOfnetProfit_TTM_ly,
                         cashOfinterest_ly,
                         assetsLiabilitiesRatio_ly,
                         tangibleAssetDebtRatio_ly,
                         cashRatio_ly,
                         totalAssets_l2y,
                         avgTotalAssets_l2y,
                         fixedAssets_l2y,
                         avgFixedAssets_l2y,
                         goodwill_l2y,
                         avgGoodwill_l2y,
                         inventory_l2y,
                         moneyFunds_l2y,
                         accountsPayable_l2y,
                         avgAccountsPayable_l2y,
                         avgInventory_l2y,
                         totalLiquidAssets_l2y,
                         avgTotalLiquidAssets_l2y,
                         totalLiabilities_l2y,
                         avgTotalLiabilities_l2y,
                         accountsReceivables_l2y,
                         avgAccountsReceivables_l2y,
                         interCompanyReceivables_l2y,
                         avgInterCompanyReceivables_l2y,
                         prepayments_l2y,
                         avgPrepayments_l2y,
                         totalCurrentLiabilities_l2y,
                         avgTotalCurrentLiabilities_l2y,
                         netCashOperatActiv_l2y,
                         cashOutInvestActiv_l2y,
                         netProfit_l2y,
                         operatingRevenue_TTM_l2y,
                         operatingCosts_TTM_l2y,
                         taxAndSurcharges_TTM_l2y,
                         salesCosts_TTM_l2y,
                         managementCosts_TTM_l2y,
                         explorationCosts_TTM_l2y,
                         financialCosts_TTM_l2y,
                         assestsDevaluation_TTM_l2y,
                         operatingProfit_TTM_l2y,
                         totalProfit_TTM_l2y,
                         incomeTax_TTM_l2y,
                         netProfit_TTM_l2y,
                         netProAftExtrGainLoss_TTM_l2y,
                         interest_TTM_l2y,
                         deprecForFixedAssets_TTM_l2y,
                         netCashOperatActiv_TTM_l2y,
                         cashOutInvestActiv_TTM_l2y,
                         ROE_l2y,
                         grossMargin_l2y,
                         ROA_l2y,
                         NetProfitMarginonSales_TTM_l2y,
                         incomeTaxRatio_l2y,
                         reinvestedIncomeRatio_l2y,
                         depreciationRatio_l2y,
                         operatingCashRatio_TTM_l2y,
                         avgFiexdOfAssets_l2y,
                         fiexdOfAssets_l2y,
                         acidTestRatio_l2y,
                         turnoverRatioOfReceivable_l2y,
                         turnoverRatioOfInventory_l2y,
                         turnoverRatioOfTotalAssets_l2y,
                         depreciationOftotalCosts_l2y,
                         cashOfnetProfit_l2y,
                         cashOfnetProfit_TTM_l2y,
                         cashOfinterest_l2y,
                         assetsLiabilitiesRatio_l2y,
                         tangibleAssetDebtRatio_l2y,
                         cashRatio_l2y,
                         totalAssets_l3y,
                         avgTotalAssets_l3y,
                         fixedAssets_l3y,
                         avgFixedAssets_l3y,
                         goodwill_l3y,
                         avgGoodwill_l3y,
                         inventory_l3y,
                         moneyFunds_l3y,
                         accountsPayable_l3y,
                         avgAccountsPayable_l3y,
                         avgInventory_l3y,
                         totalLiquidAssets_l3y,
                         avgTotalLiquidAssets_l3y,
                         totalLiabilities_l3y,
                         avgTotalLiabilities_l3y,
                         accountsReceivables_l3y,
                         avgAccountsReceivables_l3y,
                         interCompanyReceivables_l3y,
                         avgInterCompanyReceivables_l3y,
                         prepayments_l3y,
                         avgPrepayments_l3y,
                         totalCurrentLiabilities_l3y,
                         avgTotalCurrentLiabilities_l3y,
                         netCashOperatActiv_l3y,
                         cashOutInvestActiv_l3y,
                         netProfit_l3y,
                         operatingRevenue_TTM_l3y,
                         operatingCosts_TTM_l3y,
                         taxAndSurcharges_TTM_l3y,
                         salesCosts_TTM_l3y,
                         managementCosts_TTM_l3y,
                         explorationCosts_TTM_l3y,
                         financialCosts_TTM_l3y,
                         assestsDevaluation_TTM_l3y,
                         operatingProfit_TTM_l3y,
                         totalProfit_TTM_l3y,
                         incomeTax_TTM_l3y,
                         netProfit_TTM_l3y,
                         netProAftExtrGainLoss_TTM_l3y,
                         interest_TTM_l3y,
                         deprecForFixedAssets_TTM_l3y,
                         netCashOperatActiv_TTM_l3y,
                         cashOutInvestActiv_TTM_l3y,
                         ROE_l3y,
                         grossMargin_l3y,
                         ROA_l3y,
                         NetProfitMarginonSales_TTM_l3y,
                         incomeTaxRatio_l3y,
                         reinvestedIncomeRatio_l3y,
                         depreciationRatio_l3y,
                         operatingCashRatio_TTM_l3y,
                         avgFiexdOfAssets_l3y,
                         fiexdOfAssets_l3y,
                         acidTestRatio_l3y,
                         turnoverRatioOfReceivable_l3y,
                         turnoverRatioOfInventory_l3y,
                         turnoverRatioOfTotalAssets_l3y,
                         depreciationOftotalCosts_l3y,
                         cashOfnetProfit_l3y,
                         cashOfnetProfit_TTM_l3y,
                         cashOfinterest_l3y,
                         assetsLiabilitiesRatio_l3y,
                         tangibleAssetDebtRatio_l3y,
                         cashRatio_l3y,
                         totalAssets_l4y,
                         avgTotalAssets_l4y,
                         fixedAssets_l4y,
                         avgFixedAssets_l4y,
                         goodwill_l4y,
                         avgGoodwill_l4y,
                         inventory_l4y,
                         moneyFunds_l4y,
                         accountsPayable_l4y,
                         avgAccountsPayable_l4y,
                         avgInventory_l4y,
                         totalLiquidAssets_l4y,
                         avgTotalLiquidAssets_l4y,
                         totalLiabilities_l4y,
                         avgTotalLiabilities_l4y,
                         accountsReceivables_l4y,
                         avgAccountsReceivables_l4y,
                         interCompanyReceivables_l4y,
                         avgInterCompanyReceivables_l4y,
                         prepayments_l4y,
                         avgPrepayments_l4y,
                         totalCurrentLiabilities_l4y,
                         avgTotalCurrentLiabilities_l4y,
                         netCashOperatActiv_l4y,
                         cashOutInvestActiv_l4y,
                         netProfit_l4y,
                         operatingRevenue_TTM_l4y,
                         operatingCosts_TTM_l4y,
                         taxAndSurcharges_TTM_l4y,
                         salesCosts_TTM_l4y,
                         managementCosts_TTM_l4y,
                         explorationCosts_TTM_l4y,
                         financialCosts_TTM_l4y,
                         assestsDevaluation_TTM_l4y,
                         operatingProfit_TTM_l4y,
                         totalProfit_TTM_l4y,
                         incomeTax_TTM_l4y,
                         netProfit_TTM_l4y,
                         netProAftExtrGainLoss_TTM_l4y,
                         interest_TTM_l4y,
                         deprecForFixedAssets_TTM_l4y,
                         netCashOperatActiv_TTM_l4y,
                         cashOutInvestActiv_TTM_l4y,
                         ROE_l4y,
                         grossMargin_l4y,
                         ROA_l4y,
                         NetProfitMarginonSales_TTM_l4y,
                         incomeTaxRatio_l4y,
                         reinvestedIncomeRatio_l4y,
                         depreciationRatio_l4y,
                         operatingCashRatio_TTM_l4y,
                         avgFiexdOfAssets_l4y,
                         fiexdOfAssets_l4y,
                         acidTestRatio_l4y,
                         turnoverRatioOfReceivable_l4y,
                         turnoverRatioOfInventory_l4y,
                         turnoverRatioOfTotalAssets_l4y,
                         depreciationOftotalCosts_l4y,
                         cashOfnetProfit_l4y,
                         cashOfnetProfit_TTM_l4y,
                         cashOfinterest_l4y,
                         assetsLiabilitiesRatio_l4y,
                         tangibleAssetDebtRatio_l4y,
                         cashRatio_l4y,
                         totalAssets_l5y,
                         avgTotalAssets_l5y,
                         fixedAssets_l5y,
                         avgFixedAssets_l5y,
                         goodwill_l5y,
                         avgGoodwill_l5y,
                         inventory_l5y,
                         moneyFunds_l5y,
                         accountsPayable_l5y,
                         avgAccountsPayable_l5y,
                         avgInventory_l5y,
                         totalLiquidAssets_l5y,
                         avgTotalLiquidAssets_l5y,
                         totalLiabilities_l5y,
                         avgTotalLiabilities_l5y,
                         accountsReceivables_l5y,
                         avgAccountsReceivables_l5y,
                         interCompanyReceivables_l5y,
                         avgInterCompanyReceivables_l5y,
                         prepayments_l5y,
                         avgPrepayments_l5y,
                         totalCurrentLiabilities_l5y,
                         avgTotalCurrentLiabilities_l5y,
                         netCashOperatActiv_l5y,
                         cashOutInvestActiv_l5y,
                         netProfit_l5y,
                         operatingRevenue_TTM_l5y,
                         operatingCosts_TTM_l5y,
                         taxAndSurcharges_TTM_l5y,
                         salesCosts_TTM_l5y,
                         managementCosts_TTM_l5y,
                         explorationCosts_TTM_l5y,
                         financialCosts_TTM_l5y,
                         assestsDevaluation_TTM_l5y,
                         operatingProfit_TTM_l5y,
                         totalProfit_TTM_l5y,
                         incomeTax_TTM_l5y,
                         netProfit_TTM_l5y,
                         netProAftExtrGainLoss_TTM_l5y,
                         interest_TTM_l5y,
                         deprecForFixedAssets_TTM_l5y,
                         netCashOperatActiv_TTM_l5y,
                         cashOutInvestActiv_TTM_l5y,
                         ROE_l5y,
                         grossMargin_l5y,
                         ROA_l5y,
                         NetProfitMarginonSales_TTM_l5y,
                         incomeTaxRatio_l5y,
                         reinvestedIncomeRatio_l5y,
                         depreciationRatio_l5y,
                         operatingCashRatio_TTM_l5y,
                         avgFiexdOfAssets_l5y,
                         fiexdOfAssets_l5y,
                         acidTestRatio_l5y,
                         turnoverRatioOfReceivable_l5y,
                         turnoverRatioOfInventory_l5y,
                         turnoverRatioOfTotalAssets_l5y,
                         depreciationOftotalCosts_l5y,
                         cashOfnetProfit_l5y,
                         cashOfnetProfit_TTM_l5y,
                         cashOfinterest_l5y,
                         assetsLiabilitiesRatio_l5y,
                         tangibleAssetDebtRatio_l5y,
                         cashRatio_l5y,
                         totalAssets_lq,
                         avgTotalAssets_lq,
                         fixedAssets_lq,
                         avgFixedAssets_lq,
                         goodwill_lq,
                         avgGoodwill_lq,
                         inventory_lq,
                         moneyFunds_lq,
                         accountsPayable_lq,
                         avgAccountsPayable_lq,
                         avgInventory_lq,
                         totalLiquidAssets_lq,
                         avgTotalLiquidAssets_lq,
                         totalLiabilities_lq,
                         avgTotalLiabilities_lq,
                         accountsReceivables_lq,
                         avgAccountsReceivables_lq,
                         interCompanyReceivables_lq,
                         avgInterCompanyReceivables_lq,
                         prepayments_lq,
                         avgPrepayments_lq,
                         totalCurrentLiabilities_lq,
                         avgTotalCurrentLiabilities_lq,
                         netCashOperatActiv_lq,
                         cashOutInvestActiv_lq,
                         netProfit_lq,
                         operatingRevenue_TTM_lq,
                         operatingCosts_TTM_lq,
                         taxAndSurcharges_TTM_lq,
                         salesCosts_TTM_lq,
                         managementCosts_TTM_lq,
                         explorationCosts_TTM_lq,
                         financialCosts_TTM_lq,
                         assestsDevaluation_TTM_lq,
                         operatingProfit_TTM_lq,
                         totalProfit_TTM_lq,
                         incomeTax_TTM_lq,
                         netProfit_TTM_lq,
                         netProAftExtrGainLoss_TTM_lq,
                         interest_TTM_lq,
                         deprecForFixedAssets_TTM_lq,
                         netCashOperatActiv_TTM_lq,
                         cashOutInvestActiv_TTM_lq,
                         ROE_lq,
                         grossMargin_lq,
                         ROA_lq,
                         NetProfitMarginonSales_TTM_lq,
                         incomeTaxRatio_lq,
                         reinvestedIncomeRatio_lq,
                         depreciationRatio_lq,
                         operatingCashRatio_TTM_lq,
                         avgFiexdOfAssets_lq,
                         fiexdOfAssets_lq,
                         acidTestRatio_lq,
                         turnoverRatioOfReceivable_lq,
                         turnoverRatioOfInventory_lq,
                         turnoverRatioOfTotalAssets_lq,
                         depreciationOftotalCosts_lq,
                         cashOfnetProfit_lq,
                         cashOfnetProfit_TTM_lq,
                         cashOfinterest_lq,
                         assetsLiabilitiesRatio_lq,
                         tangibleAssetDebtRatio_lq,
                         cashRatio_lq
                        from (select *  
                                from stock_market_day
                               WHERE order_date >=
                                     to_date('{deal_date}','yyyy-mm-dd') - 90
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
                         and c.end_date >= a.order_date
                         left join (select code,
                                     to_date(timetomarket, 'yyyymmdd') as market_day
                                from stock_info
                               where length(timetomarket) = 8) d
                      on a.code = d.code) h) g
       where order_date = to_date('{deal_date}','yyyy-mm-dd')
            """.format(deal_date=deal_date)
        conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
        cursor = conn.cursor()
        if type == 'all' and deal_date == None:
            print("please run this job in database")
        elif type == 'day' or deal_date != None:
            cursor.execute(sql3)
            conn.commit()
            print('analysis data for {deal_date} has been stored'.format(deal_date=deal_date))
        cursor.close()
        conn.commit()
        conn.close()
