
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
                     explorationCosts,
                     incomeTax,
                     assestsDevaluation,
                     interCompanyReceivables
                from stock_financial)
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
               and a.code = d.code
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
            from (select code, report_date, min(real_date) as send_date
                    from stock_calendar
                   group by code, report_date) h)
        SELECT a.code,
               industry,
               name,
               area,
               a.report_date,
               add_months(a.report_date, -12) as lastyear,
               add_months(a.report_date, -3) as lag1,
               add_months(a.report_date, -24) as last2year,
               add_months(a.report_date, -36) as last3year,
               add_months(a.report_date, -48) as last4year,
               add_months(a.report_date, -60) as last5year,
               send_date,
               end_date,
               a.totalAssets,
               a.avgTotalAssets,
               a.fixedAssets,
               a.avgFixedAssets,
               a.goodwill,
               a.avgGoodwill,
               a.inventory,
               a.moneyFunds,
               a.accountsPayable,
               a.avgAccountsPayable,
               a.avgInventory,
               a.totalLiquidAssets,
               a.avgTotalLiquidAssets,
               a.totalLiabilities,
               a.avgTotalLiabilities,
               a.accountsReceivables,
               a.avgAccountsReceivables,
               a.interCompanyReceivables,
               a.avgInterCompanyReceivables,
               a.prepayments,
               a.avgPrepayments,
               a.totalCurrentLiabilities,
               a.avgTotalCurrentLiabilities,
               a.netCashOperatActiv,
               a.cashOutInvestActiv,
               a.netProfit,
               a.operatingRevenue_TTM,
               a.operatingCosts_TTM,
               a.taxAndSurcharges_TTM,
               a.salesCosts_TTM,
               a.managementCosts_TTM,
               a.explorationCosts_TTM,
               a.financialCosts_TTM,
               a.assestsDevaluation_TTM,
               a.operatingProfit_TTM,
               a.totalProfit_TTM,
               a.incomeTax_TTM,
               a.netProfit_TTM,
               a.netProAftExtrGainLoss_TTM,
               a.interest_TTM,
               a.deprecForFixedAssets_TTM,
               a.netCashOperatActiv_TTM,
               a.cashOutInvestActiv_TTM,
               a.ROE,
               a.grossMargin,
               a.ROA,
               a.NetProfitMarginonSales_TTM,
               a.incomeTaxRatio,
               a.reinvestedIncomeRatio,
               a.depreciationRatio,
               a.operatingCashRatio_TTM,
               a.avgFiexdOfAssets,
               a.fiexdOfAssets,
               a.acidTestRatio,
               a.turnoverRatioOfReceivable,
               a.turnoverRatioOfInventory,
               a.turnoverRatioOfTotalAssets,
               a.depreciationOftotalCosts,
               a.cashOfnetProfit,
               a.cashOfnetProfit_TTM,
               a.cashOfinterest,
               a.assetsLiabilitiesRatio,
               a.tangibleAssetDebtRatio,
               a.cashRatio,
               b.totalAssets                 as totalAssets_ly,
               b.avgTotalAssets              as avgTotalAssets_ly,
               b.fixedAssets                 as fixedAssets_ly,
               b.avgFixedAssets              as avgFixedAssets_ly,
               b.goodwill                    as goodwill_ly,
               b.avgGoodwill                 as avgGoodwill_ly,
               b.inventory                   as inventory_ly,
               b.moneyFunds                  as moneyFunds_ly,
               b.accountsPayable             as accountsPayable_ly,
               b.avgAccountsPayable          as avgAccountsPayable_ly,
               b.avgInventory                as avgInventory_ly,
               b.totalLiquidAssets           as totalLiquidAssets_ly,
               b.avgTotalLiquidAssets        as avgTotalLiquidAssets_ly,
               b.totalLiabilities            as totalLiabilities_ly,
               b.avgTotalLiabilities         as avgTotalLiabilities_ly,
               b.accountsReceivables         as accountsReceivables_ly,
               b.avgAccountsReceivables      as avgAccountsReceivables_ly,
               b.interCompanyReceivables     as interCompanyReceivables_ly,
               b.avgInterCompanyReceivables  as avgInterCompanyReceivables_ly,
               b.prepayments                 as prepayments_ly,
               b.avgPrepayments              as avgPrepayments_ly,
               b.totalCurrentLiabilities     as totalCurrentLiabilities_ly,
               b.avgTotalCurrentLiabilities  as avgTotalCurrentLiabilities_ly,
               b.netCashOperatActiv          as netCashOperatActiv_ly,
               b.cashOutInvestActiv          as cashOutInvestActiv_ly,
               b.netProfit                   as netProfit_ly,
               b.operatingRevenue_TTM        as operatingRevenue_TTM_ly,
               b.operatingCosts_TTM          as operatingCosts_TTM_ly,
               b.taxAndSurcharges_TTM        as taxAndSurcharges_TTM_ly,
               b.salesCosts_TTM              as salesCosts_TTM_ly,
               b.managementCosts_TTM         as managementCosts_TTM_ly,
               b.explorationCosts_TTM        as explorationCosts_TTM_ly,
               b.financialCosts_TTM          as financialCosts_TTM_ly,
               b.assestsDevaluation_TTM      as assestsDevaluation_TTM_ly,
               b.operatingProfit_TTM         as operatingProfit_TTM_ly,
               b.totalProfit_TTM             as totalProfit_TTM_ly,
               b.incomeTax_TTM               as incomeTax_TTM_ly,
               b.netProfit_TTM               as netProfit_TTM_ly,
               b.netProAftExtrGainLoss_TTM   as netProAftExtrGainLoss_TTM_ly,
               b.interest_TTM                as interest_TTM_ly,
               b.deprecForFixedAssets_TTM    as deprecForFixedAssets_TTM_ly,
               b.netCashOperatActiv_TTM      as netCashOperatActiv_TTM_ly,
               b.cashOutInvestActiv_TTM      as cashOutInvestActiv_TTM_ly,
               b.ROE                         as ROE_ly,
               b.grossMargin                 as grossMargin_ly,
               b.ROA                         as ROA_ly,
               b.NetProfitMarginonSales_TTM  as NetProfitMarginonSales_TTM_ly,
               b.incomeTaxRatio              as incomeTaxRatio_ly,
               b.reinvestedIncomeRatio       as reinvestedIncomeRatio_ly,
               b.depreciationRatio           as depreciationRatio_ly,
               b.operatingCashRatio_TTM      as operatingCashRatio_TTM_ly,
               b.avgFiexdOfAssets            as avgFiexdOfAssets_ly,
               b.fiexdOfAssets               as fiexdOfAssets_ly,
               b.acidTestRatio               as acidTestRatio_ly,
               b.turnoverRatioOfReceivable   as turnoverRatioOfReceivable_ly,
               b.turnoverRatioOfInventory    as turnoverRatioOfInventory_ly,
               b.turnoverRatioOfTotalAssets  as turnoverRatioOfTotalAssets_ly,
               b.depreciationOftotalCosts    as depreciationOftotalCosts_ly,
               b.cashOfnetProfit             as cashOfnetProfit_ly,
               b.cashOfnetProfit_TTM         as cashOfnetProfit_TTM_ly,
               b.cashOfinterest              as cashOfinterest_ly,
               b.assetsLiabilitiesRatio      as assetsLiabilitiesRatio_ly,
               b.tangibleAssetDebtRatio      as tangibleAssetDebtRatio_ly,
               b.cashRatio                   as cashRatio_ly,
               b1.totalAssets                as totalAssets_l2y,
               b1.avgTotalAssets             as avgTotalAssets_l2y,
               b1.fixedAssets                as fixedAssets_l2y,
               b1.avgFixedAssets             as avgFixedAssets_l2y,
               b1.goodwill                   as goodwill_l2y,
               b1.avgGoodwill                as avgGoodwill_l2y,
               b1.inventory                  as inventory_l2y,
               b1.moneyFunds                 as moneyFunds_l2y,
               b1.accountsPayable            as accountsPayable_l2y,
               b1.avgAccountsPayable         as avgAccountsPayable_l2y,
               b1.avgInventory               as avgInventory_l2y,
               b1.totalLiquidAssets          as totalLiquidAssets_l2y,
               b1.avgTotalLiquidAssets       as avgTotalLiquidAssets_l2y,
               b1.totalLiabilities           as totalLiabilities_l2y,
               b1.avgTotalLiabilities        as avgTotalLiabilities_l2y,
               b1.accountsReceivables        as accountsReceivables_l2y,
               b1.avgAccountsReceivables     as avgAccountsReceivables_l2y,
               b1.interCompanyReceivables    as interCompanyReceivables_l2y,
               b1.avgInterCompanyReceivables as avgInterCompanyReceivables_l2y,
               b1.prepayments                as prepayments_l2y,
               b1.avgPrepayments             as avgPrepayments_l2y,
               b1.totalCurrentLiabilities    as totalCurrentLiabilities_l2y,
               b1.avgTotalCurrentLiabilities as avgTotalCurrentLiabilities_l2y,
               b1.netCashOperatActiv         as netCashOperatActiv_l2y,
               b1.cashOutInvestActiv         as cashOutInvestActiv_l2y,
               b1.netProfit                  as netProfit_l2y,
               b1.operatingRevenue_TTM       as operatingRevenue_TTM_l2y,
               b1.operatingCosts_TTM         as operatingCosts_TTM_l2y,
               b1.taxAndSurcharges_TTM       as taxAndSurcharges_TTM_l2y,
               b1.salesCosts_TTM             as salesCosts_TTM_l2y,
               b1.managementCosts_TTM        as managementCosts_TTM_l2y,
               b1.explorationCosts_TTM       as explorationCosts_TTM_l2y,
               b1.financialCosts_TTM         as financialCosts_TTM_l2y,
               b1.assestsDevaluation_TTM     as assestsDevaluation_TTM_l2y,
               b1.operatingProfit_TTM        as operatingProfit_TTM_l2y,
               b1.totalProfit_TTM            as totalProfit_TTM_l2y,
               b1.incomeTax_TTM              as incomeTax_TTM_l2y,
               b1.netProfit_TTM              as netProfit_TTM_l2y,
               b1.netProAftExtrGainLoss_TTM  as netProAftExtrGainLoss_TTM_l2y,
               b1.interest_TTM               as interest_TTM_l2y,
               b1.deprecForFixedAssets_TTM   as deprecForFixedAssets_TTM_l2y,
               b1.netCashOperatActiv_TTM     as netCashOperatActiv_TTM_l2y,
               b1.cashOutInvestActiv_TTM     as cashOutInvestActiv_TTM_l2y,
               b1.ROE                        as ROE_l2y,
               b1.grossMargin                as grossMargin_l2y,
               b1.ROA                        as ROA_l2y,
               b1.NetProfitMarginonSales_TTM as NetProfitMarginonSales_TTM_l2y,
               b1.incomeTaxRatio             as incomeTaxRatio_l2y,
               b1.reinvestedIncomeRatio      as reinvestedIncomeRatio_l2y,
               b1.depreciationRatio          as depreciationRatio_l2y,
               b1.operatingCashRatio_TTM     as operatingCashRatio_TTM_l2y,
               b1.avgFiexdOfAssets           as avgFiexdOfAssets_l2y,
               b1.fiexdOfAssets              as fiexdOfAssets_l2y,
               b1.acidTestRatio              as acidTestRatio_l2y,
               b1.turnoverRatioOfReceivable  as turnoverRatioOfReceivable_l2y,
               b1.turnoverRatioOfInventory   as turnoverRatioOfInventory_l2y,
               b1.turnoverRatioOfTotalAssets as turnoverRatioOfTotalAssets_l2y,
               b1.depreciationOftotalCosts   as depreciationOftotalCosts_l2y,
               b1.cashOfnetProfit            as cashOfnetProfit_l2y,
               b1.cashOfnetProfit_TTM        as cashOfnetProfit_TTM_l2y,
               b1.cashOfinterest             as cashOfinterest_l2y,
               b1.assetsLiabilitiesRatio     as assetsLiabilitiesRatio_l2y,
               b1.tangibleAssetDebtRatio     as tangibleAssetDebtRatio_l2y,
               b1.cashRatio                  as cashRatio_l2y,
               b2.totalAssets                as totalAssets_l3y,
               b2.avgTotalAssets             as avgTotalAssets_l3y,
               b2.fixedAssets                as fixedAssets_l3y,
               b2.avgFixedAssets             as avgFixedAssets_l3y,
               b2.goodwill                   as goodwill_l3y,
               b2.avgGoodwill                as avgGoodwill_l3y,
               b2.inventory                  as inventory_l3y,
               b2.moneyFunds                 as moneyFunds_l3y,
               b2.accountsPayable            as accountsPayable_l3y,
               b2.avgAccountsPayable         as avgAccountsPayable_l3y,
               b2.avgInventory               as avgInventory_l3y,
               b2.totalLiquidAssets          as totalLiquidAssets_l3y,
               b2.avgTotalLiquidAssets       as avgTotalLiquidAssets_l3y,
               b2.totalLiabilities           as totalLiabilities_l3y,
               b2.avgTotalLiabilities        as avgTotalLiabilities_l3y,
               b2.accountsReceivables        as accountsReceivables_l3y,
               b2.avgAccountsReceivables     as avgAccountsReceivables_l3y,
               b2.interCompanyReceivables    as interCompanyReceivables_l3y,
               b2.avgInterCompanyReceivables as avgInterCompanyReceivables_l3y,
               b2.prepayments                as prepayments_l3y,
               b2.avgPrepayments             as avgPrepayments_l3y,
               b2.totalCurrentLiabilities    as totalCurrentLiabilities_l3y,
               b2.avgTotalCurrentLiabilities as avgTotalCurrentLiabilities_l3y,
               b2.netCashOperatActiv         as netCashOperatActiv_l3y,
               b2.cashOutInvestActiv         as cashOutInvestActiv_l3y,
               b2.netProfit                  as netProfit_l3y,
               b2.operatingRevenue_TTM       as operatingRevenue_TTM_l3y,
               b2.operatingCosts_TTM         as operatingCosts_TTM_l3y,
               b2.taxAndSurcharges_TTM       as taxAndSurcharges_TTM_l3y,
               b2.salesCosts_TTM             as salesCosts_TTM_l3y,
               b2.managementCosts_TTM        as managementCosts_TTM_l3y,
               b2.explorationCosts_TTM       as explorationCosts_TTM_l3y,
               b2.financialCosts_TTM         as financialCosts_TTM_l3y,
               b2.assestsDevaluation_TTM     as assestsDevaluation_TTM_l3y,
               b2.operatingProfit_TTM        as operatingProfit_TTM_l3y,
               b2.totalProfit_TTM            as totalProfit_TTM_l3y,
               b2.incomeTax_TTM              as incomeTax_TTM_l3y,
               b2.netProfit_TTM              as netProfit_TTM_l3y,
               b2.netProAftExtrGainLoss_TTM  as netProAftExtrGainLoss_TTM_l3y,
               b2.interest_TTM               as interest_TTM_l3y,
               b2.deprecForFixedAssets_TTM   as deprecForFixedAssets_TTM_l3y,
               b2.netCashOperatActiv_TTM     as netCashOperatActiv_TTM_l3y,
               b2.cashOutInvestActiv_TTM     as cashOutInvestActiv_TTM_l3y,
               b2.ROE                        as ROE_l3y,
               b2.grossMargin                as grossMargin_l3y,
               b2.ROA                        as ROA_l3y,
               b2.NetProfitMarginonSales_TTM as NetProfitMarginonSales_TTM_l3y,
               b2.incomeTaxRatio             as incomeTaxRatio_l3y,
               b2.reinvestedIncomeRatio      as reinvestedIncomeRatio_l3y,
               b2.depreciationRatio          as depreciationRatio_l3y,
               b2.operatingCashRatio_TTM     as operatingCashRatio_TTM_l3y,
               b2.avgFiexdOfAssets           as avgFiexdOfAssets_l3y,
               b2.fiexdOfAssets              as fiexdOfAssets_l3y,
               b2.acidTestRatio              as acidTestRatio_l3y,
               b2.turnoverRatioOfReceivable  as turnoverRatioOfReceivable_l3y,
               b2.turnoverRatioOfInventory   as turnoverRatioOfInventory_l3y,
               b2.turnoverRatioOfTotalAssets as turnoverRatioOfTotalAssets_l3y,
               b2.depreciationOftotalCosts   as depreciationOftotalCosts_l3y,
               b2.cashOfnetProfit            as cashOfnetProfit_l3y,
               b2.cashOfnetProfit_TTM        as cashOfnetProfit_TTM_l3y,
               b2.cashOfinterest             as cashOfinterest_l3y,
               b2.assetsLiabilitiesRatio     as assetsLiabilitiesRatio_l3y,
               b2.tangibleAssetDebtRatio     as tangibleAssetDebtRatio_l3y,
               b2.cashRatio                  as cashRatio_l3y,
               b3.totalAssets                as totalAssets_l4y,
               b3.avgTotalAssets             as avgTotalAssets_l4y,
               b3.fixedAssets                as fixedAssets_l4y,
               b3.avgFixedAssets             as avgFixedAssets_l4y,
               b3.goodwill                   as goodwill_l4y,
               b3.avgGoodwill                as avgGoodwill_l4y,
               b3.inventory                  as inventory_l4y,
               b3.moneyFunds                 as moneyFunds_l4y,
               b3.accountsPayable            as accountsPayable_l4y,
               b3.avgAccountsPayable         as avgAccountsPayable_l4y,
               b3.avgInventory               as avgInventory_l4y,
               b3.totalLiquidAssets          as totalLiquidAssets_l4y,
               b3.avgTotalLiquidAssets       as avgTotalLiquidAssets_l4y,
               b3.totalLiabilities           as totalLiabilities_l4y,
               b3.avgTotalLiabilities        as avgTotalLiabilities_l4y,
               b3.accountsReceivables        as accountsReceivables_l4y,
               b3.avgAccountsReceivables     as avgAccountsReceivables_l4y,
               b3.interCompanyReceivables    as interCompanyReceivables_l4y,
               b3.avgInterCompanyReceivables as avgInterCompanyReceivables_l4y,
               b3.prepayments                as prepayments_l4y,
               b3.avgPrepayments             as avgPrepayments_l4y,
               b3.totalCurrentLiabilities    as totalCurrentLiabilities_l4y,
               b3.avgTotalCurrentLiabilities as avgTotalCurrentLiabilities_l4y,
               b3.netCashOperatActiv         as netCashOperatActiv_l4y,
               b3.cashOutInvestActiv         as cashOutInvestActiv_l4y,
               b3.netProfit                  as netProfit_l4y,
               b3.operatingRevenue_TTM       as operatingRevenue_TTM_l4y,
               b3.operatingCosts_TTM         as operatingCosts_TTM_l4y,
               b3.taxAndSurcharges_TTM       as taxAndSurcharges_TTM_l4y,
               b3.salesCosts_TTM             as salesCosts_TTM_l4y,
               b3.managementCosts_TTM        as managementCosts_TTM_l4y,
               b3.explorationCosts_TTM       as explorationCosts_TTM_l4y,
               b3.financialCosts_TTM         as financialCosts_TTM_l4y,
               b3.assestsDevaluation_TTM     as assestsDevaluation_TTM_l4y,
               b3.operatingProfit_TTM        as operatingProfit_TTM_l4y,
               b3.totalProfit_TTM            as totalProfit_TTM_l4y,
               b3.incomeTax_TTM              as incomeTax_TTM_l4y,
               b3.netProfit_TTM              as netProfit_TTM_l4y,
               b3.netProAftExtrGainLoss_TTM  as netProAftExtrGainLoss_TTM_l4y,
               b3.interest_TTM               as interest_TTM_l4y,
               b3.deprecForFixedAssets_TTM   as deprecForFixedAssets_TTM_l4y,
               b3.netCashOperatActiv_TTM     as netCashOperatActiv_TTM_l4y,
               b3.cashOutInvestActiv_TTM     as cashOutInvestActiv_TTM_l4y,
               b3.ROE                        as ROE_l4y,
               b3.grossMargin                as grossMargin_l4y,
               b3.ROA                        as ROA_l4y,
               b3.NetProfitMarginonSales_TTM as NetProfitMarginonSales_TTM_l4y,
               b3.incomeTaxRatio             as incomeTaxRatio_l4y,
               b3.reinvestedIncomeRatio      as reinvestedIncomeRatio_l4y,
               b3.depreciationRatio          as depreciationRatio_l4y,
               b3.operatingCashRatio_TTM     as operatingCashRatio_TTM_l4y,
               b3.avgFiexdOfAssets           as avgFiexdOfAssets_l4y,
               b3.fiexdOfAssets              as fiexdOfAssets_l4y,
               b3.acidTestRatio              as acidTestRatio_l4y,
               b3.turnoverRatioOfReceivable  as turnoverRatioOfReceivable_l4y,
               b3.turnoverRatioOfInventory   as turnoverRatioOfInventory_l4y,
               b3.turnoverRatioOfTotalAssets as turnoverRatioOfTotalAssets_l4y,
               b3.depreciationOftotalCosts   as depreciationOftotalCosts_l4y,
               b3.cashOfnetProfit            as cashOfnetProfit_l4y,
               b3.cashOfnetProfit_TTM        as cashOfnetProfit_TTM_l4y,
               b3.cashOfinterest             as cashOfinterest_l4y,
               b3.assetsLiabilitiesRatio     as assetsLiabilitiesRatio_l4y,
               b3.tangibleAssetDebtRatio     as tangibleAssetDebtRatio_l4y,
               b3.cashRatio                  as cashRatio_l4y,
               b4.totalAssets                as totalAssets_l5y,
               b4.avgTotalAssets             as avgTotalAssets_l5y,
               b4.fixedAssets                as fixedAssets_l5y,
               b4.avgFixedAssets             as avgFixedAssets_l5y,
               b4.goodwill                   as goodwill_l5y,
               b4.avgGoodwill                as avgGoodwill_l5y,
               b4.inventory                  as inventory_l5y,
               b4.moneyFunds                 as moneyFunds_l5y,
               b4.accountsPayable            as accountsPayable_l5y,
               b4.avgAccountsPayable         as avgAccountsPayable_l5y,
               b4.avgInventory               as avgInventory_l5y,
               b4.totalLiquidAssets          as totalLiquidAssets_l5y,
               b4.avgTotalLiquidAssets       as avgTotalLiquidAssets_l5y,
               b4.totalLiabilities           as totalLiabilities_l5y,
               b4.avgTotalLiabilities        as avgTotalLiabilities_l5y,
               b4.accountsReceivables        as accountsReceivables_l5y,
               b4.avgAccountsReceivables     as avgAccountsReceivables_l5y,
               b4.interCompanyReceivables    as interCompanyReceivables_l5y,
               b4.avgInterCompanyReceivables as avgInterCompanyReceivables_l5y,
               b4.prepayments                as prepayments_l5y,
               b4.avgPrepayments             as avgPrepayments_l5y,
               b4.totalCurrentLiabilities    as totalCurrentLiabilities_l5y,
               b4.avgTotalCurrentLiabilities as avgTotalCurrentLiabilities_l5y,
               b4.netCashOperatActiv         as netCashOperatActiv_l5y,
               b4.cashOutInvestActiv         as cashOutInvestActiv_l5y,
               b4.netProfit                  as netProfit_l5y,
               b4.operatingRevenue_TTM       as operatingRevenue_TTM_l5y,
               b4.operatingCosts_TTM         as operatingCosts_TTM_l5y,
               b4.taxAndSurcharges_TTM       as taxAndSurcharges_TTM_l5y,
               b4.salesCosts_TTM             as salesCosts_TTM_l5y,
               b4.managementCosts_TTM        as managementCosts_TTM_l5y,
               b4.explorationCosts_TTM       as explorationCosts_TTM_l5y,
               b4.financialCosts_TTM         as financialCosts_TTM_l5y,
               b4.assestsDevaluation_TTM     as assestsDevaluation_TTM_l5y,
               b4.operatingProfit_TTM        as operatingProfit_TTM_l5y,
               b4.totalProfit_TTM            as totalProfit_TTM_l5y,
               b4.incomeTax_TTM              as incomeTax_TTM_l5y,
               b4.netProfit_TTM              as netProfit_TTM_l5y,
               b4.netProAftExtrGainLoss_TTM  as netProAftExtrGainLoss_TTM_l5y,
               b4.interest_TTM               as interest_TTM_l5y,
               b4.deprecForFixedAssets_TTM   as deprecForFixedAssets_TTM_l5y,
               b4.netCashOperatActiv_TTM     as netCashOperatActiv_TTM_l5y,
               b4.cashOutInvestActiv_TTM     as cashOutInvestActiv_TTM_l5y,
               b4.ROE                        as ROE_l5y,
               b4.grossMargin                as grossMargin_l5y,
               b4.ROA                        as ROA_l5y,
               b4.NetProfitMarginonSales_TTM as NetProfitMarginonSales_TTM_l5y,
               b4.incomeTaxRatio             as incomeTaxRatio_l5y,
               b4.reinvestedIncomeRatio      as reinvestedIncomeRatio_l5y,
               b4.depreciationRatio          as depreciationRatio_l5y,
               b4.operatingCashRatio_TTM     as operatingCashRatio_TTM_l5y,
               b4.avgFiexdOfAssets           as avgFiexdOfAssets_l5y,
               b4.fiexdOfAssets              as fiexdOfAssets_l5y,
               b4.acidTestRatio              as acidTestRatio_l5y,
               b4.turnoverRatioOfReceivable  as turnoverRatioOfReceivable_l5y,
               b4.turnoverRatioOfInventory   as turnoverRatioOfInventory_l5y,
               b4.turnoverRatioOfTotalAssets as turnoverRatioOfTotalAssets_l5y,
               b4.depreciationOftotalCosts   as depreciationOftotalCosts_l5y,
               b4.cashOfnetProfit            as cashOfnetProfit_l5y,
               b4.cashOfnetProfit_TTM        as cashOfnetProfit_TTM_l5y,
               b4.cashOfinterest             as cashOfinterest_l5y,
               b4.assetsLiabilitiesRatio     as assetsLiabilitiesRatio_l5y,
               b4.tangibleAssetDebtRatio     as tangibleAssetDebtRatio_l5y,
               b4.cashRatio                  as cashRatio_l5y,
               d.totalAssets                 as totalAssets_lq,
               d.avgTotalAssets              as avgTotalAssets_lq,
               d.fixedAssets                 as fixedAssets_lq,
               d.avgFixedAssets              as avgFixedAssets_lq,
               d.goodwill                    as goodwill_lq,
               d.avgGoodwill                 as avgGoodwill_lq,
               d.inventory                   as inventory_lq,
               d.moneyFunds                  as moneyFunds_lq,
               d.accountsPayable             as accountsPayable_lq,
               d.avgAccountsPayable          as avgAccountsPayable_lq,
               d.avgInventory                as avgInventory_lq,
               d.totalLiquidAssets           as totalLiquidAssets_lq,
               d.avgTotalLiquidAssets        as avgTotalLiquidAssets_lq,
               d.totalLiabilities            as totalLiabilities_lq,
               d.avgTotalLiabilities         as avgTotalLiabilities_lq,
               d.accountsReceivables         as accountsReceivables_lq,
               d.avgAccountsReceivables      as avgAccountsReceivables_lq,
               d.interCompanyReceivables     as interCompanyReceivables_lq,
               d.avgInterCompanyReceivables  as avgInterCompanyReceivables_lq,
               d.prepayments                 as prepayments_lq,
               d.avgPrepayments              as avgPrepayments_lq,
               d.totalCurrentLiabilities     as totalCurrentLiabilities_lq,
               d.avgTotalCurrentLiabilities  as avgTotalCurrentLiabilities_lq,
               d.netCashOperatActiv          as netCashOperatActiv_lq,
               d.cashOutInvestActiv          as cashOutInvestActiv_lq,
               d.netProfit                   as netProfit_lq,
               d.operatingRevenue_TTM        as operatingRevenue_TTM_lq,
               d.operatingCosts_TTM          as operatingCosts_TTM_lq,
               d.taxAndSurcharges_TTM        as taxAndSurcharges_TTM_lq,
               d.salesCosts_TTM              as salesCosts_TTM_lq,
               d.managementCosts_TTM         as managementCosts_TTM_lq,
               d.explorationCosts_TTM        as explorationCosts_TTM_lq,
               d.financialCosts_TTM          as financialCosts_TTM_lq,
               d.assestsDevaluation_TTM      as assestsDevaluation_TTM_lq,
               d.operatingProfit_TTM         as operatingProfit_TTM_lq,
               d.totalProfit_TTM             as totalProfit_TTM_lq,
               d.incomeTax_TTM               as incomeTax_TTM_lq,
               d.netProfit_TTM               as netProfit_TTM_lq,
               d.netProAftExtrGainLoss_TTM   as netProAftExtrGainLoss_TTM_lq,
               d.interest_TTM                as interest_TTM_lq,
               d.deprecForFixedAssets_TTM    as deprecForFixedAssets_TTM_lq,
               d.netCashOperatActiv_TTM      as netCashOperatActiv_TTM_lq,
               d.cashOutInvestActiv_TTM      as cashOutInvestActiv_TTM_lq,
               d.ROE                         as ROE_lq,
               d.grossMargin                 as grossMargin_lq,
               d.ROA                         as ROA_lq,
               d.NetProfitMarginonSales_TTM  as NetProfitMarginonSales_TTM_lq,
               d.incomeTaxRatio              as incomeTaxRatio_lq,
               d.reinvestedIncomeRatio       as reinvestedIncomeRatio_lq,
               d.depreciationRatio           as depreciationRatio_lq,
               d.operatingCashRatio_TTM      as operatingCashRatio_TTM_lq,
               d.avgFiexdOfAssets            as avgFiexdOfAssets_lq,
               d.fiexdOfAssets               as fiexdOfAssets_lq,
               d.acidTestRatio               as acidTestRatio_lq,
               d.turnoverRatioOfReceivable   as turnoverRatioOfReceivable_lq,
               d.turnoverRatioOfInventory    as turnoverRatioOfInventory_lq,
               d.turnoverRatioOfTotalAssets  as turnoverRatioOfTotalAssets_lq,
               d.depreciationOftotalCosts    as depreciationOftotalCosts_lq,
               d.cashOfnetProfit             as cashOfnetProfit_lq,
               d.cashOfnetProfit_TTM         as cashOfnetProfit_TTM_lq,
               d.cashOfinterest              as cashOfinterest_lq,
               d.assetsLiabilitiesRatio      as assetsLiabilitiesRatio_lq,
               d.tangibleAssetDebtRatio      as tangibleAssetDebtRatio_lq,
               d.cashRatio                   as cashRatio_lq
          FROM t a
          left join t b
            on b.report_date = add_months(a.report_date, -12)
           and a.code = b.code
          left join t d
            on d.report_date = add_months(a.report_date, -3)
           and a.code = d.code
          left join rp c
            on c.report_date = a.report_date
           and a.code = c.code
          left join stock_info f
            on a.code = f.code
          left join t b1
            on b1.report_date = add_months(a.report_date, -24)
           and a.code = b1.code
          left join t b2
            on b2.report_date = add_months(a.report_date, -36)
           and a.code = b2.code
          left join t b3
            on b3.report_date = add_months(a.report_date, -48)
           and a.code = b3.code
          left join t b4
            on b4.report_date = add_months(a.report_date, -60)
           and a.code = b4.code
        '''
    if type == 'day' and deal_date == None:
        deal_date

    if type == 'day' or deal_date != None:
        dateS = ''' where order_date  = to_date('{deal_date}','yyyy-mm-dd')
        '''.format(deal_date = deal_date)
        s_condition = dateS
    elif type == 'all':
        s_condition = ''

    sql3="""  select g.*,
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
         avg(ROA) over(partition by order_date) as all_ROA
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
                 sum(operatingCosts_TTM) over(partition by order_date) as all_operatingCosts_TTM
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
                         b.shares_after * 10000 as shares,
                         round(a.close * b.shares_after * 10000, 2) AS total_market,
                         round((a.amount / a.volume) * b.shares_after * 10000, 2) AS avg_total_market,
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
                           {s_condition}) a
                    left join (select code,
                                     order_date,
                                     case
                                       when shares_before = 0 then
                                        lead(shares_before)
                                        over(partition by code order by
                                             order_date)
                                       else
                                        shares_before
                                     end as shares_before,
                                     shares_after,
                                     nvl(lead(order_date)
                                         over(partition by code order by
                                              order_date),
                                         TO_DATE(to_char(SYSDATE, 'yyyy/mm/dd'),
                                                 'yyyy/mm/dd')) as end_date
                                from (select code,
                                             order_date,
                                             max(shares_after) as shares_after,
                                             max(shares_before) as shares_before
                                        FROM (select h.*
                                                from (select code,
                                                             order_date,
                                                             shares_after,
                                                             shares_before,
                                                             count(*) as abb
                                                        from stock_xdxr
                                                       where (shares_after > 0 or
                                                             shares_before > 0)
                                                         and shares_after !=
                                                             shares_before
                                                       GROUP BY code,
                                                                order_date,
                                                                shares_after,
                                                                shares_before
                                                      union
                                                      SELECT code,
                                                             to_date(timeToMarket,
                                                                     'yyyymmdd') as order_date,
                                                             0 as shares_after,
                                                             0 as share_before,
                                                             1 as abb
                                                        FROM stock_info
                                                       WHERE timeToMarket != 0) h) g
                                       group by code, order_date) m) b
                      on a.code = b.code
                     and a.order_date > b.order_date
                     and a.order_date <= b.end_date
                    left join stock_financial_analysis c
                      on a.code = c.code
                     and c.send_date < a.order_date
                     and c.end_date >= a.order_date) h) g
        """.format(s_condition=s_condition)
    if type == 'day' or deal_date != None:
        actions = 'insert into stock_analysis_data '
    elif type == 'all':
        actions = 'create table stock_analysis_data as'


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
       order_Date as "date",
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
         when TOTALLIABILITIES <= 0 then
          0
         else
          total_market / TOTALLIABILITIES
       end as PT,
       case
         when MONEYFUNDS <= 0 then
          0
         else
          total_market / MONEYFUNDS
       end as PM,
       case
         when OPERATINGREVENUE_TTM <= 0 then
          0
         else
          total_market / OPERATINGREVENUE_TTM
       end as PS,
       case
         when OPERATINGCASHRATIO_TTM <= 0 then
          0
         else
          total_market / OPERATINGCASHRATIO_TTM
       end as PSC,
       case
         when NETCASHOPERATACTIV_TTM <= 0 then
          0
         else
          total_market / NETCASHOPERATACTIV_TTM
       end as PC,
       case
         when NETPROFIT_TTM_LY <= 0 then
          0
         else
          pe / (NETPROFIT_TTM / NETPROFIT_TTM_LY - 1) / 100
       end as Peg,
       case
         when OPERATINGREVENUE_TTM_LY <= 0 then
          0
         else
          PE / (OPERATINGREVENUE_TTM / OPERATINGREVENUE_TTM_LY - 1) / 100
       end as PSG,
       
       case
         when totalassets_LY <= 0 then
          0
         else
          PE / (totalassets / totalassets_LY - 1) / 100
       end as PBG,
       
       round((case
               when PRE_MARKET <= 0 then
                0
               else
                PRE2_MARKET / PRE_MARKET - 1
             end) * 100,
             2) as target,
       round((case
               when PRE_MARKET <= 0 then
                0
               else
                PRE3_MARKET / PRE_MARKET - 1
             end) * 100,
             2) as target3,
       round((case
               when PRE_MARKET <= 0 then
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