
import cx_Oracle
import pandas as pd
import datetime
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_get_trade_range,QA_util_get_last_day,
                               QA_util_if_trade)

def QA_util_process_financial(deal_date = None, type = 'day'):

    sql1 = '''create table stock_financial_TTM as
 with f as
 (select code,
       report_date,
       totalAssets,
       intangibleAssets,
       goodwill,
       longTermDeferredExpenses,
       fixedAssets,
       interestReceivables,
       inventory,
       moneyFunds,
       accountsReceivables,
       prepayments,
       totalLiquidAssets,
       interestPayable,
       accountsPayable,
       totalCurrentLiabilities,
       totalLiabilities,
       operatingRevenue,
       operatingCosts,
       taxAndSurcharges,
       operatingProfit,
       netProfit,
       financialCosts,
       netProAftExtrGainLoss,
       cashOutOperaActiv,
       netCashOperatActiv,
       cashOutInvestActiv,
       deprecForFixedAssets,
       subsidyIncome,
       interestCoverageRatio,
       totalProfit,
       cashPayDistDivProf,
       cashPayLongTermAssets,
       dispCashLongTermAssets,
       DECODE(interestCoverageRatio,
              1,
              netProfit,
              netProfit / (interestCoverageRatio - 1)) as interest,
       cashPayDistDivProf -
       DECODE(interestCoverageRatio,
              1,
              netProfit,
              netProfit / (interestCoverageRatio - 1)) as cashPayDistDiv,
       currentRatio,
       acidTestRatio,
       cashRatio,
       salesCosts,
       managementCosts,
       incomeTax,
       assestsDevaluation,
       provisionForAssetsLosses,
       interCompanyReceivables,
       explorationCosts
  from (select code,
               report_date,
               max(totalAssets) as totalAssets,
               max(intangibleAssets) as intangibleAssets,
               max(goodwill) as goodwill,
               max(longTermDeferredExpenses) as longTermDeferredExpenses,
               max(fixedAssets) as fixedAssets,
               max(interestReceivables) as interestReceivables,
               max(inventory) as inventory,
               max(moneyFunds) as moneyFunds,
               max(accountsReceivables) as accountsReceivables,
               max(prepayments) as prepayments,
               max(totalLiquidAssets) as totalLiquidAssets,
               max(interestPayable) as interestPayable,
               max(accountsPayable) as accountsPayable,
               max(totalCurrentLiabilities) as totalCurrentLiabilities,
               max(totalLiabilities) as totalLiabilities,
               max(operatingRevenue) as operatingRevenue,
               max(operatingCosts) as operatingCosts,
               max(taxAndSurcharges) as taxAndSurcharges,
               max(operatingProfit) as operatingProfit,
               max(netProfit) as netProfit,
               max(financialCosts) as financialCosts,
               max(netProAftExtrGainLoss) as netProAftExtrGainLoss,
               max(cashOutOperaActiv) as cashOutOperaActiv,
               max(netCashOperatActiv) as netCashOperatActiv,
               max(cashOutInvestActiv) as cashOutInvestActiv,
               max(deprecForFixedAssets) as deprecForFixedAssets,
               max(subsidyIncome) as subsidyIncome,
               max(interestCoverageRatio) as interestCoverageRatio,
               max(totalProfit) as totalProfit,
               max(cashPayDistDivProf) as cashPayDistDivProf,
               max(cashPayLongTermAssets) as cashPayLongTermAssets,
               max(dispCashLongTermAssets) as dispCashLongTermAssets,
               max(currentRatio) as currentRatio,
               max(acidTestRatio) as acidTestRatio,
               max(cashRatio) as cashRatio,
               max(salesCosts) as salesCosts,
               max(managementCosts) as managementCosts,
               max(incomeTax) as incomeTax,
               max(assestsDevaluation) as assestsDevaluation,
               max(provisionForAssetsLosses) as provisionForAssetsLosses,
               max(interCompanyReceivables) as interCompanyReceivables,
               max(explorationCosts) as explorationCosts
          from (select code,
                       report_date,
                       totalAssets,
                       intangibleAssets,
                       goodwill,
                       longTermDeferredExpenses,
                       fixedAssets,
                       interestReceivables,
                       inventory,
                       moneyFunds,
                       accountsReceivables,
                       prepayments,
                       totalLiquidAssets,
                       interestPayable,
                       accountsPayable,
                       totalCurrentLiabilities,
                       totalLiabilities,
                       operatingRevenue,
                       operatingCosts,
                       taxAndSurcharges,
                       operatingProfit,
                       netProfitsBelToParComOwner as netProfit,
                       financialCosts,
                       netProAftExtrGainLoss,
                       cashOutOperaActiv,
                       netCashOperatActiv,
                       cashOutInvestActiv,
                       deprecForFixedAssets,
                       subsidyIncome,
                       interestCoverageRatio,
                       totalProfit,
                       cashPayDistDivProf,
                       cashPayLongTermAssets,
                       dispCashLongTermAssets,
                       currentRatio,
                       acidTestRatio,
                       cashRatio,
                       salesCosts,
                       managementCosts,
                       incomeTax,
                       assestsDevaluation,
                       provisionForAssetsLosses,
                       interCompanyReceivables,
                       explorationCosts
                  from stock_financial
                union all
                select code,
                       report_date,
                       totalAssets,
                       intangibleAssets,
                       goodwill,
                       longTermDeferredExpenses,
                       fixedAssets,
                       interestReceivables,
                       inventory,
                       moneyFunds,
                       accountsReceivables,
                       prepayments,
                       totalLiquidAssets,
                       interestPayable,
                       accountsPayable,
                       totalCurrentLiabilities,
                       totalLiabilities,
                       operatingRevenue,
                       operatingCosts,
                       taxAndSurcharges,
                       operatingProfit,
                       netProfitsBelToParComOwner as netProfit,
                       financialCosts,
                       netProAftExtrGainLoss,
                       cashOutOperaActiv,
                       netCashOperatActiv,
                       cashOutInvestActiv,
                       deprecForFixedAssets,
                       subsidyIncome,
                       case
                         when financialCosts <= 0 then
                          100
                         else
                          (operatingProfit + financialCosts) / financialCosts
                       end as interestCoverageRatio,
                       totalProfit,
                       cashPayDistDivProf,
                       cashPayLongTermAssets,
                       dispCashLongTermAssets,
                       case
                         when totalCurrentLiabilities <= 0 then
                          100
                         else
                          totalLiquidAssets / totalCurrentLiabilities
                       end as currentRatio,
                       case
                         when totalCurrentLiabilities <= 0 then
                          100
                         else
                          (totalLiquidAssets - inventory) /
                          totalCurrentLiabilities
                       end as acidTestRatio,
                       case
                         when totalCurrentLiabilities <= 0 then
                          100
                         else
                          (totalLiquidAssets - inventory - accountsReceivables) /
                          totalCurrentLiabilities
                       end as cashRatio,
                       salesCosts,
                       managementCosts,
                       incomeTax,
                       assestsDevaluation,
                       provisionForAssetsLosses,
                       null as interCompanyReceivables,
                       null as explorationCosts
                  from stock_financial_wy) h
         group by code, report_date) g)
  SELECT A.CODE,
A.REPORT_DATE,
ADD_MONTHS(A.REPORT_DATE, -12) AS LY,
ADD_MONTHS(A.REPORT_DATE, -3) AS LAG1,
TRUNC(A.REPORT_DATE, 'YEAR') - 1 AS YY,
(A.TOTALASSETS + B.TOTALASSETS) / 2 AS AVGTOTALASSETS,
(A.FIXEDASSETS + B.FIXEDASSETS) / 2 AS AVGFIXEDASSETS,
(A.GOODWILL + B.GOODWILL) / 2 AS AVGGOODWILL,
(A.INVENTORY + B.INVENTORY) / 2 AS AVGINVENTORY,
(A.TOTALLIQUIDASSETS + B.TOTALLIQUIDASSETS) / 2AS AVGTOTALLIQUIDASSETS,
(A.TOTALLIABILITIES + B.TOTALLIABILITIES) / 2 AS AVGTOTALLIABILITIES,
(A.ACCOUNTSRECEIVABLES + B.ACCOUNTSRECEIVABLES) / 2 AS AVGACCOUNTSRECEIVABLES,
(A.INTERCOMPANYRECEIVABLES + B.INTERCOMPANYRECEIVABLES) / 2 AS AVGINTERCOMPANYRECEIVABLES,
(A.PREPAYMENTS + B.PREPAYMENTS) / 2 AS AVGPREPAYMENTS,
(A.ACCOUNTSPAYABLE + B.ACCOUNTSPAYABLE) / 2 AS AVGACCOUNTSPAYABLE,
(A.TOTALCURRENTLIABILITIES + B.TOTALCURRENTLIABILITIES) / 2 AS AVGTOTALCURRENTLIABILITIES,
E.OPERATINGREVENUE - B.OPERATINGREVENUE + A.OPERATINGREVENUE AS OPERATINGREVENUE_TTM,
E.OPERATINGCOSTS - B.OPERATINGCOSTS + A.OPERATINGCOSTS AS OPERATINGCOSTS_TTM,
E.TAXANDSURCHARGES - B.TAXANDSURCHARGES + A.TAXANDSURCHARGES AS TAXANDSURCHARGES_TTM,
E.SALESCOSTS - B.SALESCOSTS + A.SALESCOSTS AS SALESCOSTS_TTM,
E.MANAGEMENTCOSTS - B.MANAGEMENTCOSTS + A.MANAGEMENTCOSTS AS MANAGEMENTCOSTS_TTM,
E.EXPLORATIONCOSTS - B.EXPLORATIONCOSTS + A.EXPLORATIONCOSTS AS EXPLORATIONCOSTS_TTM,
E.FINANCIALCOSTS - B.FINANCIALCOSTS + A.FINANCIALCOSTS AS FINANCIALCOSTS_TTM,
E.ASSESTSDEVALUATION - B.ASSESTSDEVALUATION + A.ASSESTSDEVALUATION AS ASSESTSDEVALUATION_TTM,
E.OPERATINGPROFIT - B.OPERATINGPROFIT + A.OPERATINGPROFIT AS OPERATINGPROFIT_TTM,
E.TOTALPROFIT - B.TOTALPROFIT + A.TOTALPROFIT AS TOTALPROFIT_TTM,
E.INCOMETAX - B.INCOMETAX + A.INCOMETAX AS INCOMETAX_TTM,
E.NETPROFIT - B.NETPROFIT + A.NETPROFIT AS NETPROFIT_TTM,
E.NETPROAFTEXTRGAINLOSS - B.NETPROAFTEXTRGAINLOSS +
A.NETPROAFTEXTRGAINLOSS AS NETPROAFTEXTRGAINLOSS_TTM,
E.INTEREST - B.INTEREST + A.INTEREST AS INTEREST_TTM,
E.DEPRECFORFIXEDASSETS - B.DEPRECFORFIXEDASSETS +
A.DEPRECFORFIXEDASSETS AS DEPRECFORFIXEDASSETS_TTM,
E.NETCASHOPERATACTIV - B.NETCASHOPERATACTIV + A.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_TTM,
E.CASHOUTINVESTACTIV - B.CASHOUTINVESTACTIV + A.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_TTM,
A.MONEYFUNDS,
A.TOTALASSETS,
A.ACCOUNTSPAYABLE,
A.INTEREST,
A.INTANGIBLEASSETS,
A.SALESCOSTS,
A.MANAGEMENTCOSTS,
A.EXPLORATIONCOSTS,
A.INCOMETAX,
A.INTERCOMPANYRECEIVABLES,
A.GOODWILL,
A.LONGTERMDEFERREDEXPENSES,
A.FIXEDASSETS,
A.INTERESTRECEIVABLES,
A.INVENTORY,
A.ACCOUNTSRECEIVABLES,
A.PREPAYMENTS,
A.TOTALLIQUIDASSETS,
A.INTERESTPAYABLE,
A.TOTALCURRENTLIABILITIES,
A.TOTALLIABILITIES,
A.OPERATINGREVENUE,
A.OPERATINGCOSTS,
A.TAXANDSURCHARGES,
A.OPERATINGPROFIT,
A.NETPROFIT,
A.FINANCIALCOSTS,
A.NETPROAFTEXTRGAINLOSS,
A.CASHOUTINVESTACTIV,
A.CASHOUTOPERAACTIV,
A.NETCASHOPERATACTIV,
A.DEPRECFORFIXEDASSETS,
A.SUBSIDYINCOME,
A.INTERESTCOVERAGERATIO,
A.TOTALPROFIT,
A.CASHPAYDISTDIVPROF,
A.CASHPAYLONGTERMASSETS,
A.NETPROFIT - A.NETPROAFTEXTRGAINLOSS AS EXTRGAINLOSS

  from (select * from f where to_char(report_date,'yyyymmdd') >= '20000101') A
  left join f b
    on b.report_date = add_months(A.report_date, -12)
   and A.code = b.code
  left join f d
    on d.report_date = add_months(A.report_date, -3)
   and A.code = d.code
  left join f e
    on e.report_date = trunc(A.report_date, 'year') - 1
   and A.code = e.code
     '''

    sql2='''create table stock_financial_analysis as with t as
 (select code,
         report_date,
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
         cashOutInvestActiv_TTM
         ---盈利能力
        ,
         case
           when avgTotalAssets - avgGoodwill - avgtotalLiabilities <= 0 then
            0
           else
            netProAftExtrGainLoss_TTM /
            (avgTotalAssets - avgGoodwill - avgtotalLiabilities)
         end as ROE,
         case
           when operatingCosts_TTM <= 0 then
            0
           else
            operatingRevenue_TTM / operatingCosts_TTM - 1
         end as grossMargin,
         case
           when avgTotalAssets - avgGoodwill <= 0 then
            0
           else
            netProAftExtrGainLoss_TTM / (avgTotalAssets - avgGoodwill)
         end as ROA,
         case
           when operatingRevenue_TTM <= 0 then
            0
           else
            (netProAftExtrGainLoss_TTM) / (operatingRevenue_TTM)
         end as NetProfitMarginonSales_TTM,
         case
           when netProAftExtrGainLoss_TTM <= 0 then
            0
           else
            incomeTax_TTM / netProAftExtrGainLoss_TTM
         end as incomeTaxRatio,
         case
           when operatingRevenue_TTM <= 0 then
            0
           else
            cashOutInvestActiv_TTM / operatingRevenue_TTM
         end as reinvestedIncomeRatio,
         case
           when operatingRevenue_TTM <= 0 then
            0
           else
            deprecForFixedAssets_TTM / operatingRevenue_TTM
         end as depreciationRatio,
         case
           when operatingRevenue_TTM <= 0 then
            0
           else
            (accountsReceivables + inventory - accountsPayable) /
            operatingRevenue_TTM
         end AS operatingCashRatio_TTM
         
         ---运营模式
        ,
         case
           when avgTotalAssets - avgGoodwill <= 0 then
            0
           else
            avgFixedAssets / (avgTotalAssets - avgGoodwill)
         end AS avgFiexdOfAssets,
         case
           when avgTotalAssets <= 0 then
            0
           else
            avgFixedAssets / avgTotalAssets
         end AS fiexdOfAssets,
         case
           when avgTotalCurrentLiabilities <= 0 then
            0
           else
            (avgTotalLiquidAssets - avgInventory - avgPrepayments) /
            avgTotalCurrentLiabilities
         end AS acidTestRatio
         
         ---运营效率
        ,
         case
           when avgAccountsReceivables <= 0 then
            0
           else
            operatingRevenue_TTM / avgAccountsReceivables
         end as turnoverRatioOfReceivable,
         case
           when avgInventory <= 0 then
            0
           else
            operatingRevenue_TTM / avgInventory
         end as turnoverRatioOfInventory,
         case
           when avgTotalAssets <= 0 then
            0
           else
            operatingRevenue_TTM / avgTotalAssets
         end as turnoverRatioOfTotalAssets,
         case
           when operatingRevenue_TTM - totalProfit_TTM <= 0 then
            0
           else
            deprecForFixedAssets_TTM /
            (operatingRevenue_TTM - totalProfit_TTM)
         end as depreciationOftotalCosts
         
         ---利润质量
        ,
         case
           when netProfit <= 0 then
            0
           else
            netCashOperatActiv / netProfit
         end AS cashOfnetProfit,
         case
           when netProfit_TTM <= 0 then
            0
           else
            netCashOperatActiv_TTM / netProfit_TTM
         end AS cashOfnetProfit_TTM,
         case
           when interest_TTM <= 0 then
            0
           else
            netCashOperatActiv_TTM / interest_TTM
         end as cashOfinterest
         
         ---偿债能力
        ,
         case
           when totalAssets <= 0 then
            0
           else
            totalLiabilities / totalAssets
         end as assetsLiabilitiesRatio,
         case
           when totalCurrentLiabilities <= 0 then
            0
           else
            totalLiabilities / totalCurrentLiabilities
         end as tangibleAssetDebtRatio,
         case
           when totalCurrentLiabilities <= 0 then
            0
           else
            (totalLiquidAssets - inventory - accountsReceivables -
            prepayments) / totalCurrentLiabilities
         end AS cashRatio
    from stock_financial_TTM),
rp as
 (select code,
         to_date(report_date, 'yyyy-mm-dd') as report_date,
         to_date(send_date, 'yyyy-mm-dd') as send_date,
         to_date(nvl(lead(send_date, 1)
                     over(partition by code order by report_date),
                     to_char(sysdate, 'yyyy-mm-dd')),
                 'yyyy-mm-dd') as end_date
    from (select code,
                 to_char(report_date, 'yyyy-mm-dd') as report_date,
                 to_char(MIN(real_date), 'yyyy-mm-dd') as send_date
            from stock_calendar
           group by code, report_date) h),
res as
 (SELECT A.CODE,
         INDUSTRY,
         NAME,
         AREA,
         A.REPORT_DATE,
         ADD_MONTHS(A.REPORT_DATE, -12) AS LASTYEAR,
         ADD_MONTHS(A.REPORT_DATE, -3) AS LAG1,
         ADD_MONTHS(A.REPORT_DATE, -24) AS LAST2YEAR,
         ADD_MONTHS(A.REPORT_DATE, -36) AS LAST3YEAR,
         ADD_MONTHS(A.REPORT_DATE, -48) AS LAST4YEAR,
         ADD_MONTHS(A.REPORT_DATE, -60) AS LAST5YEAR,
         SEND_DATE,
         END_DATE,
         A.TOTALASSETS,
         A.AVGTOTALASSETS,
         A.FIXEDASSETS,
         A.AVGFIXEDASSETS,
         A.GOODWILL,
         A.AVGGOODWILL,
         A.INVENTORY,
         A.MONEYFUNDS,
         A.ACCOUNTSPAYABLE,
         A.AVGACCOUNTSPAYABLE,
         A.AVGINVENTORY,
         A.TOTALLIQUIDASSETS,
         A.AVGTOTALLIQUIDASSETS,
         A.TOTALLIABILITIES,
         A.AVGTOTALLIABILITIES,
         A.ACCOUNTSRECEIVABLES,
         A.AVGACCOUNTSRECEIVABLES,
         A.INTERCOMPANYRECEIVABLES,
         A.AVGINTERCOMPANYRECEIVABLES,
         A.PREPAYMENTS,
         A.AVGPREPAYMENTS,
         A.TOTALCURRENTLIABILITIES,
         A.AVGTOTALCURRENTLIABILITIES,
         A.NETCASHOPERATACTIV,
         A.CASHOUTINVESTACTIV,
         A.NETPROFIT,
         A.OPERATINGREVENUE_TTM,
         A.OPERATINGCOSTS_TTM,
         A.TAXANDSURCHARGES_TTM,
         A.SALESCOSTS_TTM,
         A.MANAGEMENTCOSTS_TTM,
         A.EXPLORATIONCOSTS_TTM,
         A.FINANCIALCOSTS_TTM,
         A.ASSESTSDEVALUATION_TTM,
         A.OPERATINGPROFIT_TTM,
         A.TOTALPROFIT_TTM,
         A.INCOMETAX_TTM,
         A.NETPROFIT_TTM,
         A.NETPROAFTEXTRGAINLOSS_TTM,
         A.INTEREST_TTM,
         A.DEPRECFORFIXEDASSETS_TTM,
         A.NETCASHOPERATACTIV_TTM,
         A.CASHOUTINVESTACTIV_TTM,
         A.ROE,
         A.GROSSMARGIN,
         A.ROA,
         A.NETPROFITMARGINONSALES_TTM,
         A.INCOMETAXRATIO,
         A.REINVESTEDINCOMERATIO,
         A.DEPRECIATIONRATIO,
         A.OPERATINGCASHRATIO_TTM,
         A.AVGFIEXDOFASSETS,
         A.FIEXDOFASSETS,
         A.ACIDTESTRATIO,
         A.TURNOVERRATIOOFRECEIVABLE,
         A.TURNOVERRATIOOFINVENTORY,
         A.TURNOVERRATIOOFTOTALASSETS,
         A.DEPRECIATIONOFTOTALCOSTS,
         A.CASHOFNETPROFIT,
         A.CASHOFNETPROFIT_TTM,
         A.CASHOFINTEREST,
         A.ASSETSLIABILITIESRATIO,
         A.TANGIBLEASSETDEBTRATIO,
         A.CASHRATIO
    FROM T A
    LEFT JOIN RP C
      ON C.REPORT_DATE = A.REPORT_DATE
     AND A.CODE = C.CODE
    LEFT JOIN STOCK_INFO F
      ON A.CODE = F.CODE)
SELECT A.CODE,
       A.INDUSTRY,
       A.NAME,
       A.AREA,
       A.REPORT_DATE,
       ADD_MONTHS(A.REPORT_DATE, -12) AS LASTYEAR,
       ADD_MONTHS(A.REPORT_DATE, -3) AS LAG1,
       ADD_MONTHS(A.REPORT_DATE, -24) AS LAST2YEAR,
       ADD_MONTHS(A.REPORT_DATE, -36) AS LAST3YEAR,
       ADD_MONTHS(A.REPORT_DATE, -48) AS LAST4YEAR,
       ADD_MONTHS(A.REPORT_DATE, -60) AS LAST5YEAR,
       A.SEND_DATE,
       A.END_DATE,
       A.TOTALASSETS,
       A.AVGTOTALASSETS,
       A.FIXEDASSETS,
       A.AVGFIXEDASSETS,
       A.GOODWILL,
       A.AVGGOODWILL,
       A.INVENTORY,
       A.MONEYFUNDS,
       A.ACCOUNTSPAYABLE,
       A.AVGACCOUNTSPAYABLE,
       A.AVGINVENTORY,
       A.TOTALLIQUIDASSETS,
       A.AVGTOTALLIQUIDASSETS,
       A.TOTALLIABILITIES,
       A.AVGTOTALLIABILITIES,
       A.ACCOUNTSRECEIVABLES,
       A.AVGACCOUNTSRECEIVABLES,
       A.INTERCOMPANYRECEIVABLES,
       A.AVGINTERCOMPANYRECEIVABLES,
       A.PREPAYMENTS,
       A.AVGPREPAYMENTS,
       A.TOTALCURRENTLIABILITIES,
       A.AVGTOTALCURRENTLIABILITIES,
       A.NETCASHOPERATACTIV,
       A.CASHOUTINVESTACTIV,
       A.NETPROFIT,
       A.OPERATINGREVENUE_TTM,
       A.OPERATINGCOSTS_TTM,
       A.TAXANDSURCHARGES_TTM,
       A.SALESCOSTS_TTM,
       A.MANAGEMENTCOSTS_TTM,
       A.EXPLORATIONCOSTS_TTM,
       A.FINANCIALCOSTS_TTM,
       A.ASSESTSDEVALUATION_TTM,
       A.OPERATINGPROFIT_TTM,
       A.TOTALPROFIT_TTM,
       A.INCOMETAX_TTM,
       A.NETPROFIT_TTM,
       A.NETPROAFTEXTRGAINLOSS_TTM,
       A.INTEREST_TTM,
       A.DEPRECFORFIXEDASSETS_TTM,
       A.NETCASHOPERATACTIV_TTM,
       A.CASHOUTINVESTACTIV_TTM,
       A.ROE,
       A.GROSSMARGIN,
       A.ROA,
       A.NETPROFITMARGINONSALES_TTM,
       A.INCOMETAXRATIO,
       A.REINVESTEDINCOMERATIO,
       A.DEPRECIATIONRATIO,
       A.OPERATINGCASHRATIO_TTM,
       A.AVGFIEXDOFASSETS,
       A.FIEXDOFASSETS,
       A.ACIDTESTRATIO,
       A.TURNOVERRATIOOFRECEIVABLE,
       A.TURNOVERRATIOOFINVENTORY,
       A.TURNOVERRATIOOFTOTALASSETS,
       A.DEPRECIATIONOFTOTALCOSTS,
       A.CASHOFNETPROFIT,
       A.CASHOFNETPROFIT_TTM,
       A.CASHOFINTEREST,
       A.ASSETSLIABILITIESRATIO,
       A.TANGIBLEASSETDEBTRATIO,
       A.CASHRATIO,
       B.TOTALASSETS AS TOTALASSETS_LY,
       B.AVGTOTALASSETS AS AVGTOTALASSETS_LY,
       B.FIXEDASSETS AS FIXEDASSETS_LY,
       B.AVGFIXEDASSETS AS AVGFIXEDASSETS_LY,
       B.GOODWILL AS GOODWILL_LY,
       B.AVGGOODWILL AS AVGGOODWILL_LY,
       B.INVENTORY AS INVENTORY_LY,
       B.MONEYFUNDS AS MONEYFUNDS_LY,
       B.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_LY,
       B.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_LY,
       B.AVGINVENTORY AS AVGINVENTORY_LY,
       B.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_LY,
       B.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_LY,
       B.TOTALLIABILITIES AS TOTALLIABILITIES_LY,
       B.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_LY,
       B.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_LY,
       B.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_LY,
       B.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_LY,
       B.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_LY,
       B.PREPAYMENTS AS PREPAYMENTS_LY,
       B.AVGPREPAYMENTS AS AVGPREPAYMENTS_LY,
       B.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_LY,
       B.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_LY,
       B.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_LY,
       B.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_LY,
       B.NETPROFIT AS NETPROFIT_LY,
       B.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_LY,
       B.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_LY,
       B.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_LY,
       B.SALESCOSTS_TTM AS SALESCOSTS_TTM_LY,
       B.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_LY,
       B.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_LY,
       B.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_LY,
       B.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_LY,
       B.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_LY,
       B.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_LY,
       B.INCOMETAX_TTM AS INCOMETAX_TTM_LY,
       B.NETPROFIT_TTM AS NETPROFIT_TTM_LY,
       B.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_LY,
       B.INTEREST_TTM AS INTEREST_TTM_LY,
       B.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_LY,
       B.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_LY,
       B.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_LY,
       B.ROE AS ROE_LY,
       B.GROSSMARGIN AS GROSSMARGIN_LY,
       B.ROA AS ROA_LY,
       B.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_LY,
       B.INCOMETAXRATIO AS INCOMETAXRATIO_LY,
       B.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_LY,
       B.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_LY,
       B.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_LY,
       B.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_LY,
       B.FIEXDOFASSETS AS FIEXDOFASSETS_LY,
       B.ACIDTESTRATIO AS ACIDTESTRATIO_LY,
       B.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_LY,
       B.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_LY,
       B.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_LY,
       B.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_LY,
       B.CASHOFNETPROFIT AS CASHOFNETPROFIT_LY,
       B.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_LY,
       B.CASHOFINTEREST AS CASHOFINTEREST_LY,
       B.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_LY,
       B.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_LY,
       B.CASHRATIO AS CASHRATIO_LY,
       B1.TOTALASSETS AS TOTALASSETS_L2Y,
       B1.AVGTOTALASSETS AS AVGTOTALASSETS_L2Y,
       B1.FIXEDASSETS AS FIXEDASSETS_L2Y,
       B1.AVGFIXEDASSETS AS AVGFIXEDASSETS_L2Y,
       B1.GOODWILL AS GOODWILL_L2Y,
       B1.AVGGOODWILL AS AVGGOODWILL_L2Y,
       B1.INVENTORY AS INVENTORY_L2Y,
       B1.MONEYFUNDS AS MONEYFUNDS_L2Y,
       B1.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_L2Y,
       B1.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_L2Y,
       B1.AVGINVENTORY AS AVGINVENTORY_L2Y,
       B1.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_L2Y,
       B1.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_L2Y,
       B1.TOTALLIABILITIES AS TOTALLIABILITIES_L2Y,
       B1.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_L2Y,
       B1.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_L2Y,
       B1.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_L2Y,
       B1.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_L2Y,
       B1.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_L2Y,
       B1.PREPAYMENTS AS PREPAYMENTS_L2Y,
       B1.AVGPREPAYMENTS AS AVGPREPAYMENTS_L2Y,
       B1.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_L2Y,
       B1.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_L2Y,
       B1.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_L2Y,
       B1.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_L2Y,
       B1.NETPROFIT AS NETPROFIT_L2Y,
       B1.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_L2Y,
       B1.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_L2Y,
       B1.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_L2Y,
       B1.SALESCOSTS_TTM AS SALESCOSTS_TTM_L2Y,
       B1.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_L2Y,
       B1.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_L2Y,
       B1.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_L2Y,
       B1.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_L2Y,
       B1.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_L2Y,
       B1.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_L2Y,
       B1.INCOMETAX_TTM AS INCOMETAX_TTM_L2Y,
       B1.NETPROFIT_TTM AS NETPROFIT_TTM_L2Y,
       B1.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_L2Y,
       B1.INTEREST_TTM AS INTEREST_TTM_L2Y,
       B1.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_L2Y,
       B1.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_L2Y,
       B1.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_L2Y,
       B1.ROE AS ROE_L2Y,
       B1.GROSSMARGIN AS GROSSMARGIN_L2Y,
       B1.ROA AS ROA_L2Y,
       B1.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_L2Y,
       B1.INCOMETAXRATIO AS INCOMETAXRATIO_L2Y,
       B1.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_L2Y,
       B1.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_L2Y,
       B1.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_L2Y,
       B1.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_L2Y,
       B1.FIEXDOFASSETS AS FIEXDOFASSETS_L2Y,
       B1.ACIDTESTRATIO AS ACIDTESTRATIO_L2Y,
       B1.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_L2Y,
       B1.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_L2Y,
       B1.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_L2Y,
       B1.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_L2Y,
       B1.CASHOFNETPROFIT AS CASHOFNETPROFIT_L2Y,
       B1.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_L2Y,
       B1.CASHOFINTEREST AS CASHOFINTEREST_L2Y,
       B1.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_L2Y,
       B1.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_L2Y,
       B1.CASHRATIO AS CASHRATIO_L2Y,
       B2.TOTALASSETS AS TOTALASSETS_L3Y,
       B2.AVGTOTALASSETS AS AVGTOTALASSETS_L3Y,
       B2.FIXEDASSETS AS FIXEDASSETS_L3Y,
       B2.AVGFIXEDASSETS AS AVGFIXEDASSETS_L3Y,
       B2.GOODWILL AS GOODWILL_L3Y,
       B2.AVGGOODWILL AS AVGGOODWILL_L3Y,
       B2.INVENTORY AS INVENTORY_L3Y,
       B2.MONEYFUNDS AS MONEYFUNDS_L3Y,
       B2.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_L3Y,
       B2.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_L3Y,
       B2.AVGINVENTORY AS AVGINVENTORY_L3Y,
       B2.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_L3Y,
       B2.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_L3Y,
       B2.TOTALLIABILITIES AS TOTALLIABILITIES_L3Y,
       B2.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_L3Y,
       B2.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_L3Y,
       B2.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_L3Y,
       B2.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_L3Y,
       B2.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_L3Y,
       B2.PREPAYMENTS AS PREPAYMENTS_L3Y,
       B2.AVGPREPAYMENTS AS AVGPREPAYMENTS_L3Y,
       B2.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_L3Y,
       B2.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_L3Y,
       B2.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_L3Y,
       B2.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_L3Y,
       B2.NETPROFIT AS NETPROFIT_L3Y,
       B2.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_L3Y,
       B2.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_L3Y,
       B2.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_L3Y,
       B2.SALESCOSTS_TTM AS SALESCOSTS_TTM_L3Y,
       B2.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_L3Y,
       B2.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_L3Y,
       B2.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_L3Y,
       B2.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_L3Y,
       B2.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_L3Y,
       B2.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_L3Y,
       B2.INCOMETAX_TTM AS INCOMETAX_TTM_L3Y,
       B2.NETPROFIT_TTM AS NETPROFIT_TTM_L3Y,
       B2.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_L3Y,
       B2.INTEREST_TTM AS INTEREST_TTM_L3Y,
       B2.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_L3Y,
       B2.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_L3Y,
       B2.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_L3Y,
       B2.ROE AS ROE_L3Y,
       B2.GROSSMARGIN AS GROSSMARGIN_L3Y,
       B2.ROA AS ROA_L3Y,
       B2.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_L3Y,
       B2.INCOMETAXRATIO AS INCOMETAXRATIO_L3Y,
       B2.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_L3Y,
       B2.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_L3Y,
       B2.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_L3Y,
       B2.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_L3Y,
       B2.FIEXDOFASSETS AS FIEXDOFASSETS_L3Y,
       B2.ACIDTESTRATIO AS ACIDTESTRATIO_L3Y,
       B2.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_L3Y,
       B2.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_L3Y,
       B2.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_L3Y,
       B2.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_L3Y,
       B2.CASHOFNETPROFIT AS CASHOFNETPROFIT_L3Y,
       B2.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_L3Y,
       B2.CASHOFINTEREST AS CASHOFINTEREST_L3Y,
       B2.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_L3Y,
       B2.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_L3Y,
       B2.CASHRATIO AS CASHRATIO_L3Y,
       B3.TOTALASSETS AS TOTALASSETS_L4Y,
       B3.AVGTOTALASSETS AS AVGTOTALASSETS_L4Y,
       B3.FIXEDASSETS AS FIXEDASSETS_L4Y,
       B3.AVGFIXEDASSETS AS AVGFIXEDASSETS_L4Y,
       B3.GOODWILL AS GOODWILL_L4Y,
       B3.AVGGOODWILL AS AVGGOODWILL_L4Y,
       B3.INVENTORY AS INVENTORY_L4Y,
       B3.MONEYFUNDS AS MONEYFUNDS_L4Y,
       B3.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_L4Y,
       B3.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_L4Y,
       B3.AVGINVENTORY AS AVGINVENTORY_L4Y,
       B3.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_L4Y,
       B3.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_L4Y,
       B3.TOTALLIABILITIES AS TOTALLIABILITIES_L4Y,
       B3.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_L4Y,
       B3.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_L4Y,
       B3.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_L4Y,
       B3.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_L4Y,
       B3.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_L4Y,
       B3.PREPAYMENTS AS PREPAYMENTS_L4Y,
       B3.AVGPREPAYMENTS AS AVGPREPAYMENTS_L4Y,
       B3.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_L4Y,
       B3.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_L4Y,
       B3.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_L4Y,
       B3.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_L4Y,
       B3.NETPROFIT AS NETPROFIT_L4Y,
       B3.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_L4Y,
       B3.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_L4Y,
       B3.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_L4Y,
       B3.SALESCOSTS_TTM AS SALESCOSTS_TTM_L4Y,
       B3.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_L4Y,
       B3.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_L4Y,
       B3.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_L4Y,
       B3.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_L4Y,
       B3.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_L4Y,
       B3.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_L4Y,
       B3.INCOMETAX_TTM AS INCOMETAX_TTM_L4Y,
       B3.NETPROFIT_TTM AS NETPROFIT_TTM_L4Y,
       B3.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_L4Y,
       B3.INTEREST_TTM AS INTEREST_TTM_L4Y,
       B3.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_L4Y,
       B3.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_L4Y,
       B3.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_L4Y,
       B3.ROE AS ROE_L4Y,
       B3.GROSSMARGIN AS GROSSMARGIN_L4Y,
       B3.ROA AS ROA_L4Y,
       B3.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_L4Y,
       B3.INCOMETAXRATIO AS INCOMETAXRATIO_L4Y,
       B3.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_L4Y,
       B3.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_L4Y,
       B3.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_L4Y,
       B3.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_L4Y,
       B3.FIEXDOFASSETS AS FIEXDOFASSETS_L4Y,
       B3.ACIDTESTRATIO AS ACIDTESTRATIO_L4Y,
       B3.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_L4Y,
       B3.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_L4Y,
       B3.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_L4Y,
       B3.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_L4Y,
       B3.CASHOFNETPROFIT AS CASHOFNETPROFIT_L4Y,
       B3.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_L4Y,
       B3.CASHOFINTEREST AS CASHOFINTEREST_L4Y,
       B3.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_L4Y,
       B3.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_L4Y,
       B3.CASHRATIO AS CASHRATIO_L4Y,
       B4.TOTALASSETS AS TOTALASSETS_L5Y,
       B4.AVGTOTALASSETS AS AVGTOTALASSETS_L5Y,
       B4.FIXEDASSETS AS FIXEDASSETS_L5Y,
       B4.AVGFIXEDASSETS AS AVGFIXEDASSETS_L5Y,
       B4.GOODWILL AS GOODWILL_L5Y,
       B4.AVGGOODWILL AS AVGGOODWILL_L5Y,
       B4.INVENTORY AS INVENTORY_L5Y,
       B4.MONEYFUNDS AS MONEYFUNDS_L5Y,
       B4.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_L5Y,
       B4.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_L5Y,
       B4.AVGINVENTORY AS AVGINVENTORY_L5Y,
       B4.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_L5Y,
       B4.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_L5Y,
       B4.TOTALLIABILITIES AS TOTALLIABILITIES_L5Y,
       B4.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_L5Y,
       B4.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_L5Y,
       B4.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_L5Y,
       B4.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_L5Y,
       B4.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_L5Y,
       B4.PREPAYMENTS AS PREPAYMENTS_L5Y,
       B4.AVGPREPAYMENTS AS AVGPREPAYMENTS_L5Y,
       B4.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_L5Y,
       B4.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_L5Y,
       B4.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_L5Y,
       B4.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_L5Y,
       B4.NETPROFIT AS NETPROFIT_L5Y,
       B4.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_L5Y,
       B4.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_L5Y,
       B4.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_L5Y,
       B4.SALESCOSTS_TTM AS SALESCOSTS_TTM_L5Y,
       B4.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_L5Y,
       B4.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_L5Y,
       B4.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_L5Y,
       B4.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_L5Y,
       B4.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_L5Y,
       B4.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_L5Y,
       B4.INCOMETAX_TTM AS INCOMETAX_TTM_L5Y,
       B4.NETPROFIT_TTM AS NETPROFIT_TTM_L5Y,
       B4.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_L5Y,
       B4.INTEREST_TTM AS INTEREST_TTM_L5Y,
       B4.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_L5Y,
       B4.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_L5Y,
       B4.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_L5Y,
       B4.ROE AS ROE_L5Y,
       B4.GROSSMARGIN AS GROSSMARGIN_L5Y,
       B4.ROA AS ROA_L5Y,
       B4.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_L5Y,
       B4.INCOMETAXRATIO AS INCOMETAXRATIO_L5Y,
       B4.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_L5Y,
       B4.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_L5Y,
       B4.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_L5Y,
       B4.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_L5Y,
       B4.FIEXDOFASSETS AS FIEXDOFASSETS_L5Y,
       B4.ACIDTESTRATIO AS ACIDTESTRATIO_L5Y,
       B4.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_L5Y,
       B4.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_L5Y,
       B4.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_L5Y,
       B4.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_L5Y,
       B4.CASHOFNETPROFIT AS CASHOFNETPROFIT_L5Y,
       B4.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_L5Y,
       B4.CASHOFINTEREST AS CASHOFINTEREST_L5Y,
       B4.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_L5Y,
       B4.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_L5Y,
       B4.CASHRATIO AS CASHRATIO_L5Y,
       D.TOTALASSETS AS TOTALASSETS_LQ,
       D.AVGTOTALASSETS AS AVGTOTALASSETS_LQ,
       D.FIXEDASSETS AS FIXEDASSETS_LQ,
       D.AVGFIXEDASSETS AS AVGFIXEDASSETS_LQ,
       D.GOODWILL AS GOODWILL_LQ,
       D.AVGGOODWILL AS AVGGOODWILL_LQ,
       D.INVENTORY AS INVENTORY_LQ,
       D.MONEYFUNDS AS MONEYFUNDS_LQ,
       D.ACCOUNTSPAYABLE AS ACCOUNTSPAYABLE_LQ,
       D.AVGACCOUNTSPAYABLE AS AVGACCOUNTSPAYABLE_LQ,
       D.AVGINVENTORY AS AVGINVENTORY_LQ,
       D.TOTALLIQUIDASSETS AS TOTALLIQUIDASSETS_LQ,
       D.AVGTOTALLIQUIDASSETS AS AVGTOTALLIQUIDASSETS_LQ,
       D.TOTALLIABILITIES AS TOTALLIABILITIES_LQ,
       D.AVGTOTALLIABILITIES AS AVGTOTALLIABILITIES_LQ,
       D.ACCOUNTSRECEIVABLES AS ACCOUNTSRECEIVABLES_LQ,
       D.AVGACCOUNTSRECEIVABLES AS AVGACCOUNTSRECEIVABLES_LQ,
       D.INTERCOMPANYRECEIVABLES AS INTERCOMPANYRECEIVABLES_LQ,
       D.AVGINTERCOMPANYRECEIVABLES AS AVGINTERCOMPANYRECEIVABLES_LQ,
       D.PREPAYMENTS AS PREPAYMENTS_LQ,
       D.AVGPREPAYMENTS AS AVGPREPAYMENTS_LQ,
       D.TOTALCURRENTLIABILITIES AS TOTALCURRENTLIABILITIES_LQ,
       D.AVGTOTALCURRENTLIABILITIES AS AVGTOTALCURRENTLIABILITIES_LQ,
       D.NETCASHOPERATACTIV AS NETCASHOPERATACTIV_LQ,
       D.CASHOUTINVESTACTIV AS CASHOUTINVESTACTIV_LQ,
       D.NETPROFIT AS NETPROFIT_LQ,
       D.OPERATINGREVENUE_TTM AS OPERATINGREVENUE_TTM_LQ,
       D.OPERATINGCOSTS_TTM AS OPERATINGCOSTS_TTM_LQ,
       D.TAXANDSURCHARGES_TTM AS TAXANDSURCHARGES_TTM_LQ,
       D.SALESCOSTS_TTM AS SALESCOSTS_TTM_LQ,
       D.MANAGEMENTCOSTS_TTM AS MANAGEMENTCOSTS_TTM_LQ,
       D.EXPLORATIONCOSTS_TTM AS EXPLORATIONCOSTS_TTM_LQ,
       D.FINANCIALCOSTS_TTM AS FINANCIALCOSTS_TTM_LQ,
       D.ASSESTSDEVALUATION_TTM AS ASSESTSDEVALUATION_TTM_LQ,
       D.OPERATINGPROFIT_TTM AS OPERATINGPROFIT_TTM_LQ,
       D.TOTALPROFIT_TTM AS TOTALPROFIT_TTM_LQ,
       D.INCOMETAX_TTM AS INCOMETAX_TTM_LQ,
       D.NETPROFIT_TTM AS NETPROFIT_TTM_LQ,
       D.NETPROAFTEXTRGAINLOSS_TTM AS NETPROAFTEXTRGAINLOSS_TTM_LQ,
       D.INTEREST_TTM AS INTEREST_TTM_LQ,
       D.DEPRECFORFIXEDASSETS_TTM AS DEPRECFORFIXEDASSETS_TTM_LQ,
       D.NETCASHOPERATACTIV_TTM AS NETCASHOPERATACTIV_TTM_LQ,
       D.CASHOUTINVESTACTIV_TTM AS CASHOUTINVESTACTIV_TTM_LQ,
       D.ROE AS ROE_LQ,
       D.GROSSMARGIN AS GROSSMARGIN_LQ,
       D.ROA AS ROA_LQ,
       D.NETPROFITMARGINONSALES_TTM AS NETPROFITMARGINONSALES_TTM_LQ,
       D.INCOMETAXRATIO AS INCOMETAXRATIO_LQ,
       D.REINVESTEDINCOMERATIO AS REINVESTEDINCOMERATIO_LQ,
       D.DEPRECIATIONRATIO AS DEPRECIATIONRATIO_LQ,
       D.OPERATINGCASHRATIO_TTM AS OPERATINGCASHRATIO_TTM_LQ,
       D.AVGFIEXDOFASSETS AS AVGFIEXDOFASSETS_LQ,
       D.FIEXDOFASSETS AS FIEXDOFASSETS_LQ,
       D.ACIDTESTRATIO AS ACIDTESTRATIO_LQ,
       D.TURNOVERRATIOOFRECEIVABLE AS TURNOVERRATIOOFRECEIVABLE_LQ,
       D.TURNOVERRATIOOFINVENTORY AS TURNOVERRATIOOFINVENTORY_LQ,
       D.TURNOVERRATIOOFTOTALASSETS AS TURNOVERRATIOOFTOTALASSETS_LQ,
       D.DEPRECIATIONOFTOTALCOSTS AS DEPRECIATIONOFTOTALCOSTS_LQ,
       D.CASHOFNETPROFIT AS CASHOFNETPROFIT_LQ,
       D.CASHOFNETPROFIT_TTM AS CASHOFNETPROFIT_TTM_LQ,
       D.CASHOFINTEREST AS CASHOFINTEREST_LQ,
       D.ASSETSLIABILITIESRATIO AS ASSETSLIABILITIESRATIO_LQ,
       D.TANGIBLEASSETDEBTRATIO AS TANGIBLEASSETDEBTRATIO_LQ,
       D.CASHRATIO AS CASHRATIO_LQ
  FROM res A
  LEFT JOIN res B
    ON B.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -12)
   AND A.CODE = B.CODE
  LEFT JOIN res D
    ON D.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -3)
   AND A.CODE = D.CODE
  LEFT JOIN res B1
    ON B1.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -24)
   AND A.CODE = B1.CODE
  LEFT JOIN res B2
    ON B2.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -36)
   AND A.CODE = B2.CODE
  LEFT JOIN res B3
    ON B3.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -48)
   AND A.CODE = B3.CODE
  LEFT JOIN res B4
    ON B4.REPORT_DATE = ADD_MONTHS(A.REPORT_DATE, -60)
   AND A.CODE = B4.CODE
        '''
    if type == 'day' and deal_date == None:
        deal_date

    if type == 'day' or deal_date != None:
        dateS = ''' where order_date  = to_date('{deal_date}','yyyy-mm-dd')
        '''.format(deal_date = deal_date)
        s_condition = dateS
    elif type == 'all':
        s_condition = ''

    sql3="""insert into stock_analysis_data
  select g.*,
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
                i_netProAftExtrGainLoss_TTM /
                (i_avgTotalAssets - i_avgGoodwill)) as i_ROA_total,
         
         avg(PE) over(partition by order_date, industry) as i_PE,
         avg(PB) over(partition by order_date, industry) as i_PB,
         avg(grossMargin) over(partition by order_date, industry) as i_grossMargin,
         avg(ROE) over(partition by order_date, industry) as i_ROE,
         avg(ROA) over(partition by order_date, industry) as i_ROA,
         
         decode(all_netProAftExtrGainLoss_TTM,
                0,
                0,
                all_total_market / all_netProAftExtrGainLoss_TTM) as all_PE_total,
         decode(all_avgTotalAssets - all_avgGoodwill -
                all_avgTotalLiabilities,
                0,
                0,
                all_total_market / (all_avgTotalAssets - all_avgGoodwill -
                all_avgTotalLiabilities)) as all_PB_total,
         decode(all_avgTotalAssets - all_avgGoodwill -
                all_avgTotalLiabilities,
                0,
                0,
                all_netProAftExtrGainLoss_TTM /
                (all_avgTotalAssets - all_avgGoodwill -
                all_avgTotalLiabilities)) as all_ROE_total,
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
         AVG(all_total_market) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_AT_MARKET,
         
         AVG(all_total_market) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_AT_MARKET,
         
         AVG(all_total_market) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_AT_MARKET,
         
         AVG(all_total_market) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_AT_MARKET,
         AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
         
         AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
         
         AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT,
         
         AVG(all_AMOUNT) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG5_AMOUNT
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
                 LAG(TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG_MARKET,
                 LAG(TOTAL_MARKET, 2) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG2_MARKET,
                 LAG(TOTAL_MARKET, 3) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG3_MARKET,
                 LAG(TOTAL_MARKET, 5) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG5_MARKET,
                 LAG(TOTAL_MARKET, 20) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG20_MARKET,
                 LAG(TOTAL_MARKET, 30) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG30_MARKET,
                 LAG(TOTAL_MARKET, 60) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS LAG60_MARKET,
                 LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE_MARKET,
                 LAG(AVG_TOTAL_MARKET, 2) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE2_MARKET,
                 LAG(AVG_TOTAL_MARKET, 3) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE3_MARKET,
                 LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE5_MARKET,
                 LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG_MARKET,
                 LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG5_MARKET,
                 LAG(AVG_TOTAL_MARKET, 20) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG20_MARKET,
                 LAG(AVG_TOTAL_MARKET, 30) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG30_MARKET,
                 LAG(AVG_TOTAL_MARKET, 60) OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG60_MARKET,
                 
                 AVG(TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_T_MARKET,
                 
                 AVG(TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_T_MARKET,
                 
                 AVG(TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_T_MARKET,
                 
                 AVG(TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_T_MARKET,
                 
                 AVG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_A_MARKET,
                 
                 AVG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_A_MARKET,
                 
                 AVG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_A_MARKET,
                 
                 AVG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_A_MARKET,
                 
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS AVG5_TOR,
                 
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 19 PRECEDING AND CURRENT ROW) AS AVG20_TOR,
                 
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS AVG30_TOR,
                 
                 AVG(TURNOVERRATIO) OVER(PARTITION BY CODE ORDER BY ORDER_DATE RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) AS AVG60_TOR,
                 decode(LAG(TOTAL_MARKET, 60)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC),
                        0,
                        0,
                        (MAX(high_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) -
                         MIN(low_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 59 PRECEDING AND CURRENT ROW)) /
                        LAG(TOTAL_MARKET, 60)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) AS RNG_60,
                 decode(LAG(TOTAL_MARKET, 30)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC),
                        0,
                        0,
                        (MAX(high_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) -
                         MIN(low_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 29 PRECEDING AND CURRENT ROW)) /
                        LAG(TOTAL_MARKET, 30)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) AS RNG_30,
                 decode(LAG(TOTAL_MARKET, 20)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC),
                        0,
                        0,
                        (MAX(high_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) -
                         MIN(low_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 19 PRECEDING AND CURRENT ROW)) /
                        LAG(TOTAL_MARKET, 20)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) AS RNG_20,
                 decode(LAG(TOTAL_MARKET, 5)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC),
                        0,
                        0,
                        (MAX(high_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 59 PRECEDING AND CURRENT ROW) -
                         MIN(low_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE
                              RANGE BETWEEN 4 PRECEDING AND CURRENT ROW)) /
                        LAG(TOTAL_MARKET, 5)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) AS RNG_5,
                 decode(LAG(TOTAL_MARKET, 2)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC),
                        0,
                        0,
                        (LAG(high_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC) -
                         LAG(low_market)
                         OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) /
                        LAG(TOTAL_MARKET, 2)
                        OVER(PARTITION BY CODE ORDER BY ORDER_DATE ASC)) AS RNG_L,
                 sum(amount) over(partition by order_Date) as all_amount
            from (select a.code,
                         a.order_date,
                         report_date,
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
                         a.open,
                         a.high,
                         a.low,
                         a.close,
                         a.volume as vol,
                         a.amount,
                         a.amount / a.volume as avgrage,
                         b.shares_after * 10000 as shares,
                         DECODE(b.shares_after * 10000,
                                0,
                                0,
                                a.volume / b.shares_after / 100) as turnoverRatio,
                         round(a.close * b.shares_after * 10000, 2) AS total_market,
                         round(a.open * b.shares_after * 10000, 2) AS open_market,
                         round(a.high * b.shares_after * 10000, 2) AS high_market,
                         round(a.low * b.shares_after * 10000, 2) AS low_market,
                         round((a.amount / a.volume) * b.shares_after * 10000,
                               2) AS avg_total_market,
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
                    from (select * where 
                            from stock_market_day
                           WHERE order_date >=
                                 to_date('{deal_date}','yyyy-mm-dd') - 90
                             ) a
                    left join (select code,
                                     begin_date as order_date,
                                     nvl(LAG(begin_date)
                                         OVER(PARTITION BY CODE ORDER BY
                                              begin_date desc),
                                         TO_DATE(to_char(SYSDATE, 'yyyy/mm/dd'),
                                                 'yyyy/mm/dd') + 1) AS end_date,
                                     total_shares as shares_after,
                                     LAG(total_shares) OVER(PARTITION BY CODE ORDER BY begin_date ASC) AS shares_before
                                from (select code,
                                             begin_date,
                                             max(total_shares) as total_shares
                                        from stock_shares
                                       group by code, begin_date) h) b
                      on a.code = b.code
                     and a.order_date >= b.order_date
                     and a.order_date < b.end_date
                    left join stock_financial_analysis c
                      on a.code = c.code
                     and c.send_date < a.order_date
                     and c.end_date >= a.order_date) h) g
   where order_date = to_date('{deal_date}','yyyy-mm-dd')
        """.format(deal_date=deal_date)
    if type == 'day' or deal_date != None:
        actions = 'insert into stock_analysis_data '
    elif type == 'all':
        print("please run this job in database")


    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    cursor = conn.cursor()
    cursor.execute('''drop table stock_financial_TTM''')
    cursor.execute(sql1)
    conn.commit()
    cursor.execute('''drop table stock_financial_analysis''')
    cursor.execute(sql2)
    conn.commit()
    print("financial TTM report has been stored")
    if type == 'all' and deal_date == None:
        cursor.execute('''drop table stock_analysis_data''')
        sql3 = actions + sql3
        cursor.execute(sql3)
        conn.commit()
        print('analysis data has been stored')
    elif type == 'day' or deal_date != None:
        sql3 = actions + sql3
        cursor.execute(sql3)
        conn.commit()
        print('analysis data for {deal_date} has been stored'.format(deal_date=deal_date))
    cursor.close()
    conn.commit()
    conn.close()


def QA_util_process_financial2(start_date, end_date):

    sql1 = """INSERT INTO QUANT_ANALYSIS_DATA
  select *
    from (select A.*,
                 TO_DATE(TO_CHAR(TIMETOMARKET), 'YYYYMMDD') AS MARKET_DAY,
                 LAG(TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE_MARKET,
                 LAG(TOTAL_MARKET, 2) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE2_MARKET,
                 LAG(TOTAL_MARKET, 3) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE3_MARKET,
                 LAG(TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE5_MARKET,
                 LAG(TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS LAG_MARKET,
                 LAG(TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS LAG5_MARKET,
                 LAG(TOTAL_MARKET, 20) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS LAG20_MARKET,
                 LAG(TOTAL_MARKET, 30) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS LAG30_MARKET,
                 LAG(TOTAL_MARKET, 60) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS LAG60_MARKET,
                 LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE_MARKET,
                   LAG(AVG_TOTAL_MARKET, 2) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE2_MARKET,
                   LAG(AVG_TOTAL_MARKET, 3) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE3_MARKET,
                   LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE5_MARKET,
                   LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG_MARKET,
                   LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG5_MARKET,
                   LAG(AVG_TOTAL_MARKET, 20) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG20_MARKET,
                   LAG(AVG_TOTAL_MARKET, 30) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG30_MARKET,
                   LAG(AVG_TOTAL_MARKET, 60) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE ASC) AS AVG_LAG60_MARKET,
                 CASE
                   WHEN ORDER_DATE -
                        TO_DATE(TO_CHAR(TIMETOMARKET), 'YYYYMMDD') <= 5 THEN
                    0
                   ELSE
                    AVG(TURNOVERRATIO)
                    OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE
                         RANGE BETWEEN 4 PRECEDING AND CURRENT ROW)
                 END AS LAG5_TOR,
                 CASE
                   WHEN ORDER_DATE -
                        TO_DATE(TO_CHAR(TIMETOMARKET), 'YYYYMMDD') <= 20 THEN
                    0
                   ELSE
                    AVG(TURNOVERRATIO)
                    OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE
                         RANGE BETWEEN 19 PRECEDING AND CURRENT ROW)
                 END AS LAG20_TOR,
                 CASE
                   WHEN ORDER_DATE -
                        TO_DATE(TO_CHAR(TIMETOMARKET), 'YYYYMMDD') <= 30 THEN
                    0
                   ELSE
                    AVG(TURNOVERRATIO)
                    OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE
                         RANGE BETWEEN 29 PRECEDING AND CURRENT ROW)
                 END AS LAG30_TOR,
                 CASE
                   WHEN ORDER_DATE -
                        TO_DATE(TO_CHAR(TIMETOMARKET), 'YYYYMMDD') <= 60 THEN
                    0
                   ELSE
                    AVG(TURNOVERRATIO)
                    OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE
                         RANGE BETWEEN 59 PRECEDING AND CURRENT ROW)
                 END AS LAG60_TOR
            from (select a.*,
                         DECODE(shares, 0, 0, vol / shares * 100) as turnoverRatio
                    from stock_analysis_data a where order_date >= (to_date('{start_date}', 'yyyy-mm-dd') - 120)
           and order_date <= to_date('{start_date}', 'yyyy-mm-dd')
                    ) A
          left join stock_info B
            on A.CODE = B.CODE ) h
        where ORDER_DATE = (to_date('{start_date}', 'yyyy-mm-dd')-10)"""

    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    cursor = conn.cursor()
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while start_date <= end_date:
        if QA_util_if_trade(start_date.strftime("%Y-%m-%d")) == True:
            print(start_date.strftime("%Y-%m-%d"), QA_util_get_last_day(start_date.strftime("%Y-%m-%d"),6))
            sql2 = sql1.format(start_date=start_date.strftime("%Y-%m-%d"))
            if QA_util_get_last_day(start_date.strftime("%Y-%m-%d"),6) == 'wrong date':
                print(sql2)
            cursor.execute(sql2)
            print('quant analysis data for {deal_date} has been stored'.format(deal_date=start_date.strftime("%Y-%m-%d")))
            start_date = start_date+datetime.timedelta(days=1)
            conn.commit()
        else:
            print("not a trading day")
            start_date = start_date+datetime.timedelta(days=1)
    cursor.close()
    conn.commit()
    conn.close()

def QA_util_etl_financial_TTM():

    sql = '''select CODE,
                INDUSTRY,
                NAME,
                AREA,
                TO_CHAR(REPORT_DATE, 'YYYY-MM-DD') AS REPORT_DATE,
                TO_CHAR(LASTYEAR, 'YYYY-MM-DD') AS LASTYEAR,
                TO_CHAR(LAG1, 'YYYY-MM-DD') AS LAG1,
                TO_CHAR(LAST2YEAR, 'YYYY-MM-DD') AS LAST2YEAR,
                TO_CHAR(LAST3YEAR, 'YYYY-MM-DD') AS LAST3YEAR,
                TO_CHAR(LAST4YEAR, 'YYYY-MM-DD') AS LAST4YEAR,
                TO_CHAR(LAST5YEAR, 'YYYY-MM-DD') AS LAST5YEAR,
                TO_CHAR(SEND_DATE, 'YYYY-MM-DD') AS SEND_DATE,
                TO_CHAR(END_DATE, 'YYYY-MM-DD') AS END_DATE,
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
                CASHRATIO_LQ
  from stock_financial_analysis
    '''
    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    data = pd.read_sql(sql=sql, con=conn)
    data = data.assign(date_stamp=data['REPORT_DATE'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    conn.close()
    return(data)

def QA_util_etl_stock_financial(start_date):
    sql = '''select code,
       name,
       industry,
       to_char(order_date,'yyyy-mm-dd') as "date",
       open,
       high,
       low,
       close,
       vol,
       amount,
       shares,
       total_market,
       pe,
       pb,
       case
         when TOTALLIABILITIES = 0 then
          0
         else
          total_market / TOTALLIABILITIES
       end as PT,
       case
         when MONEYFUNDS = 0 then
          0
         else
          total_market / MONEYFUNDS
       end as PM,
       case
         when OPERATINGREVENUE_TTM = 0 then
          0
         else
          total_market / OPERATINGREVENUE_TTM
       end as PS,
       case
         when OPERATINGCASHRATIO_TTM = 0 then
          0
         else
          total_market / OPERATINGCASHRATIO_TTM
       end as PSC,
       case
         when NETCASHOPERATACTIV_TTM = 0 then
          0
         else
          total_market / NETCASHOPERATACTIV_TTM
       end as PC,
       case
         when NETPROFIT_TTM_LY = 0 or NETPROFIT_TTM = NETPROFIT_TTM_LY then
          0
         else
          pe / (NETPROFIT_TTM / NETPROFIT_TTM_LY - 1) / 100
       end as Peg,
       case
         when OPERATINGREVENUE_TTM_LY = 0 or OPERATINGREVENUE_TTM = OPERATINGREVENUE_TTM_LY then
          0
         else
          PE / (OPERATINGREVENUE_TTM / OPERATINGREVENUE_TTM_LY - 1) / 100
       end as PSG,
       
       case
         when totalassets_LY = 0 or totalassets = totalassets_LY then
          0
         else
          PE / (totalassets / totalassets_LY - 1) / 100
       end as PBG,
       
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
             2) as target5
  from QUANT_ANALYSIS_DATA A
 where order_date = (to_date('{start_date}', 'yyyy-mm-dd')-10)'''.format(start_date=start_date)
    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    data = pd.read_sql(sql=sql, con=conn)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    conn.close()
    if data.shape[0] == 0:
        print("No data For {start_date}".format(start_date=start_date))
        return None
    else:
        return(data)