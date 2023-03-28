import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select *
  from (select b.code,
               b.name,
               b.industry,
               b.total_market,
               b.grossMargin,
               b.turnoverRatioOfTotalAssets,
               b.operatingRinrate,
               b.pb,
               b.pe_ttm,
               b.roe_ttm,
               a.index_code as "index",
               a.BLN
          from (select code as index_code, stock as code, index_name as BLN
                  from index_stock
                 where code not like '8802%'
                 and cate in ('2', '4')
                   and stock is not null) a
          left join (
                    select a.code,
                            name,
                            industry,
                            round(total_market / 100000000, 2) as total_market,
                            round(tra_total_market / total_market * 100, 2) AS tra_rate,
                            round(pe_ttm, 2) AS pe_ttm,
                            round(peegl_ttm, 2) AS peegl_ttm,
                            round(pb, 2) AS pb,
                            round(roe * 100, 2) AS roe,
                            round(roe_ttm * 100, 2) AS roe_ttm,
                            round(netroe * 100, 2) AS netroe,
                            round(netroe_ttm * 100, 2) AS netroe_ttm,
                            round((roe_yoy + roe_l2y + roe_l3y + roe_l4y) / 5,
                                  2) as roe_avg5,
                            round((roa_yoy + roa_l2y + roa_l3y + roa_l4y) / 5,
                                  2) as roa_avg5,
                            round((grossMargin_yoy + grossMargin_l2y +
                                  grossMargin_l3y + grossMargin_l4y) / 5,
                                  2) as gross_avg5,
                            round(least(roe_yoy + roe_l2y + roe_l3y + roe_l4y),
                                  2) as roe_min,
                            round(least(roa_yoy + roa_l2y + roa_l3y + roa_l4y),
                                  2) as roa_min,
                            round(least(grossMargin_yoy + grossMargin_l2y +
                                        grossMargin_l3y + grossMargin_l4y),
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
                                    when grossMargin_yoy + grossMargin_l2y +
                                         grossMargin_l3y + grossMargin_l4y = 0 then
                                     0
                                    else
                                     grossMargin / (grossMargin_yoy + grossMargin_l2y +
                                     grossMargin_l3y + grossMargin_l4y)
                                  end,
                                  2) as gross_ch,
                            round(roe_yoy * 100, 2) AS roe_yoy,
                            round(roe_l2y * 100, 2) AS roe_l2y,
                            round(roe_l3y * 100, 2) AS roe_l3y,
                            round(roe_l4y * 100, 2) AS roe_l4y,
                            round(roa * 100, 2) AS roa,
                            round(grossMargin * 100, 2) AS grossMargin,
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
                                    when netProfit_TTM > 0 and netProfit_TTM_yoy > 0 then
                                     (netProfit_TTM - netProfit_TTM_yoy) /
                                     netProfit_TTM_yoy
                                    when netProfit_TTM > 0 and netProfit_TTM_yoy < 0 then
                                     - ((netProfit_TTM - netProfit_TTM_yoy) /
                                        netProfit_TTM_yoy)
                                    when netProfit_TTM < 0 and netProfit_TTM_yoy > 0 then
                                     ((netProfit_TTM - netProfit_TTM_yoy) /
                                     netProfit_TTM_yoy)
                                    when netProfit_TTM < 0 and netProfit_TTM_yoy < 0 then
                                     - ((netProfit_TTM - netProfit_TTM_yoy) /
                                        netProfit_TTM_yoy)
                                  end * 100,
                                  2) as netProfit_inrate,
                            round(case
                                    when netProfit_TTM_l2y = 0 then
                                     0
                                    when netProfit_TTM_yoy > 0 and
                                         netProfit_TTM_l2y > 0 then
                                     (netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                     netProfit_TTM_l2y
                                    when netProfit_TTM_yoy > 0 and
                                         netProfit_TTM_l2y < 0 then
                                     - ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                        netProfit_TTM_l2y)
                                    when netProfit_TTM_yoy < 0 and
                                         netProfit_TTM_l2y > 0 then
                                     ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                     netProfit_TTM_l2y)
                                    when netProfit_TTM_yoy < 0 and
                                         netProfit_TTM_l2y < 0 then
                                     - ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                        netProfit_TTM_l2y)
                                  end * 100,
                                  2) as netProfit_inrate_yoy,
                            round(case
                                    when netProfit_TTM_l2y = 0 then
                                     0
                                    when netProfit_TTM_l2y > 0 and
                                         netProfit_TTM_l3y > 0 then
                                     (netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                     netProfit_TTM_l3y
                                    when netProfit_TTM_l2y > 0 and
                                         netProfit_TTM_l3y < 0 then
                                     - ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                        netProfit_TTM_l3y)
                                    when netProfit_TTM_l2y < 0 and
                                         netProfit_TTM_l3y > 0 then
                                     ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                     netProfit_TTM_l3y)
                                    when netProfit_TTM_l2y < 0 and
                                         netProfit_TTM_l3y < 0 then
                                     - ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                        netProfit_TTM_l3y)
                                  end * 100,
                                  2) as netProfit_inrate_l2y,
                            round(case
                                    when netProfit_TTM_l3y = 0 then
                                     0
                                    when netProfit_TTM_l3y > 0 and
                                         netProfit_TTM_l4y > 0 then
                                     (netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                     netProfit_TTM_l4y
                                    when netProfit_TTM_l3y > 0 and
                                         netProfit_TTM_l4y < 0 then
                                     - ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                        netProfit_TTM_l4y)
                                    when netProfit_TTM_l3y < 0 and
                                         netProfit_TTM_l4y > 0 then
                                     ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                     netProfit_TTM_l4y)
                                    when netProfit_TTM_l3y < 0 and
                                         netProfit_TTM_l4y < 0 then
                                     - ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                        netProfit_TTM_l4y)
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
                                    when totalProfit > 0 and totalProfit_yoy > 0 then
                                     (totalProfit - totalProfit_yoy) /
                                     totalProfit_yoy
                                    when totalProfit > 0 and totalProfit_yoy < 0 then
                                     - ((totalProfit - totalProfit_yoy) /
                                        totalProfit_yoy)
                                    when totalProfit < 0 and totalProfit_yoy > 0 then
                                     ((totalProfit - totalProfit_yoy) /
                                     totalProfit_yoy)
                                    when totalProfit < 0 and totalProfit_yoy < 0 then
                                     - ((totalProfit - totalProfit_yoy) /
                                        totalProfit_yoy)
                                  end * 100,
                                  2) as totalProfitinrate,
                            round(case
                                    when totalProfit_l2y = 0 then
                                     0
                                    when totalProfit_yoy > 0 and totalProfit_l2y > 0 then
                                     (totalProfit_yoy - totalProfit_l2y) /
                                     totalProfit_l2y
                                    when totalProfit_yoy > 0 and totalProfit_l2y < 0 then
                                     - ((totalProfit_yoy - totalProfit_l2y) /
                                        totalProfit_l2y)
                                    when totalProfit_yoy < 0 and totalProfit_l2y > 0 then
                                     ((totalProfit_yoy - totalProfit_l2y) /
                                     totalProfit_l2y)
                                    when totalProfit_yoy < 0 and totalProfit_l2y < 0 then
                                     - ((totalProfit_yoy - totalProfit_l2y) /
                                        totalProfit_l2y)
                                  end * 100,
                                  2) as totalProfitinrate_yoy,
                            round(case
                                    when totalProfit_l3y = 0 then
                                     0
                                    when totalProfit_l2y > 0 and totalProfit_l3y > 0 then
                                     (totalProfit_l2y - totalProfit_l3y) /
                                     totalProfit_l3y
                                    when totalProfit_l2y > 0 and totalProfit_l3y < 0 then
                                     - ((totalProfit_l2y - totalProfit_l3y) /
                                        totalProfit_l3y)
                                    when totalProfit_l2y < 0 and totalProfit_l3y > 0 then
                                     ((totalProfit_l2y - totalProfit_l3y) /
                                     totalProfit_l3y)
                                    when totalProfit_l2y < 0 and totalProfit_l3y < 0 then
                                     - ((totalProfit_l2y - totalProfit_l3y) /
                                        totalProfit_l3y)
                                  end * 100,
                                  2) as totalProfitinrate_l2y,
                            round(case
                                    when totalProfit_l4y = 0 then
                                     0
                                    when totalProfit_l3y > 0 and totalProfit_l4y > 0 then
                                     (totalProfit_l3y - totalProfit_l4y) /
                                     totalProfit_l4y
                                    when totalProfit_l3y > 0 and totalProfit_l4y < 0 then
                                     - ((totalProfit_l3y - totalProfit_l4y) /
                                        totalProfit_l4y)
                                    when totalProfit_l3y < 0 and totalProfit_l4y > 0 then
                                     ((totalProfit_l3y - totalProfit_l4y) /
                                     totalProfit_l4y)
                                    when totalProfit_l3y < 0 and totalProfit_l4y < 0 then
                                     - ((totalProfit_l3y - totalProfit_l4y) /
                                        totalProfit_l4y)
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
                                    when NETPROFIT_TTM_yoy <= 0 or
                                         NETPROFIT_TTM = NETPROFIT_TTM_yoy then
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
                                     pe_ttm /
                                     (OPERATINGREVENUE / OPERATINGREVENUE_yoy - 1) / 100
                                  end * 100,
                                  2) as PSG,
                            round(case
                                    when totalassets_yoy = 0 or
                                         totalassets = totalassets_yoy then
                                     0
                                    else
                                     pe_ttm / (totalassets / totalassets_yoy - 1) / 100
                                  end * 100,
                                  2) as PBG
                      from stock_analysis_data a
                     where order_date >=
                           to_date('{from_}', 'yyyy-mm-dd')
                           and order_date <=
                           to_date('{to_}', 'yyyy-mm-dd')) b
            on a.code = b.code) h
 where code is not null
'''

sql_text1 = '''select *
  from (select b.code,
               b.name,
               b.industry,
               b.total_market,
               b.grossMargin,
               b.turnoverRatioOfTotalAssets,
               b.operatingRinrate,
               b.pb,
               b.pe_ttm,
               b.roe_ttm,
               a.blk,
               a.blockname
          from (select type as blk, code, blockname
                  from stock_block
                 where type in ('TDXNHY', 'TDXRSHY', 'gn')
                   and code is not null) a
          left join (select a.code,
                           name,
                           industry,
                           round(total_market / 100000000, 2) as total_market,
                           round(tra_total_market / total_market * 100, 2) AS tra_rate,
                           round(pe_ttm, 2) AS pe_ttm,
                           round(peegl_ttm, 2) AS peegl_ttm,
                           round(pb, 2) AS pb,
                           round(roe * 100, 2) AS roe,
                           round(roe_ttm * 100, 2) AS roe_ttm,
                           round(netroe * 100, 2) AS netroe,
                           round(netroe_ttm * 100, 2) AS netroe_ttm,
                           round((roe_yoy + roe_l2y + roe_l3y + roe_l4y) / 5,
                                 2) as roe_avg5,
                           round((roa_yoy + roa_l2y + roa_l3y + roa_l4y) / 5,
                                 2) as roa_avg5,
                           round((grossMargin_yoy + grossMargin_l2y +
                                 grossMargin_l3y + grossMargin_l4y) / 5,
                                 2) as gross_avg5,
                           round(least(roe_yoy + roe_l2y + roe_l3y + roe_l4y),
                                 2) as roe_min,
                           round(least(roa_yoy + roa_l2y + roa_l3y + roa_l4y),
                                 2) as roa_min,
                           round(least(grossMargin_yoy + grossMargin_l2y +
                                       grossMargin_l3y + grossMargin_l4y),
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
                                   when grossMargin_yoy + grossMargin_l2y +
                                        grossMargin_l3y + grossMargin_l4y = 0 then
                                    0
                                   else
                                    grossMargin /
                                    (grossMargin_yoy + grossMargin_l2y +
                                    grossMargin_l3y + grossMargin_l4y)
                                 end,
                                 2) as gross_ch,
                           round(roe_yoy * 100, 2) AS roe_yoy,
                           round(roe_l2y * 100, 2) AS roe_l2y,
                           round(roe_l3y * 100, 2) AS roe_l3y,
                           round(roe_l4y * 100, 2) AS roe_l4y,
                           round(roa * 100, 2) AS roa,
                           round(grossMargin * 100, 2) AS grossMargin,
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
                                   when netProfit_TTM > 0 and netProfit_TTM_yoy > 0 then
                                    (netProfit_TTM - netProfit_TTM_yoy) /
                                    netProfit_TTM_yoy
                                   when netProfit_TTM > 0 and netProfit_TTM_yoy < 0 then
                                    - ((netProfit_TTM - netProfit_TTM_yoy) /
                                       netProfit_TTM_yoy)
                                   when netProfit_TTM < 0 and netProfit_TTM_yoy > 0 then
                                    ((netProfit_TTM - netProfit_TTM_yoy) /
                                    netProfit_TTM_yoy)
                                   when netProfit_TTM < 0 and netProfit_TTM_yoy < 0 then
                                    - ((netProfit_TTM - netProfit_TTM_yoy) /
                                       netProfit_TTM_yoy)
                                 end * 100,
                                 2) as netProfit_inrate,
                           round(case
                                   when netProfit_TTM_l2y = 0 then
                                    0
                                   when netProfit_TTM_yoy > 0 and
                                        netProfit_TTM_l2y > 0 then
                                    (netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                    netProfit_TTM_l2y
                                   when netProfit_TTM_yoy > 0 and
                                        netProfit_TTM_l2y < 0 then
                                    - ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                       netProfit_TTM_l2y)
                                   when netProfit_TTM_yoy < 0 and
                                        netProfit_TTM_l2y > 0 then
                                    ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                    netProfit_TTM_l2y)
                                   when netProfit_TTM_yoy < 0 and
                                        netProfit_TTM_l2y < 0 then
                                    - ((netProfit_TTM_yoy - netProfit_TTM_l2y) /
                                       netProfit_TTM_l2y)
                                 end * 100,
                                 2) as netProfit_inrate_yoy,
                           round(case
                                   when netProfit_TTM_l2y = 0 then
                                    0
                                   when netProfit_TTM_l2y > 0 and
                                        netProfit_TTM_l3y > 0 then
                                    (netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                    netProfit_TTM_l3y
                                   when netProfit_TTM_l2y > 0 and
                                        netProfit_TTM_l3y < 0 then
                                    - ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                       netProfit_TTM_l3y)
                                   when netProfit_TTM_l2y < 0 and
                                        netProfit_TTM_l3y > 0 then
                                    ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                    netProfit_TTM_l3y)
                                   when netProfit_TTM_l2y < 0 and
                                        netProfit_TTM_l3y < 0 then
                                    - ((netProfit_TTM_l2y - netProfit_TTM_l3y) /
                                       netProfit_TTM_l3y)
                                 end * 100,
                                 2) as netProfit_inrate_l2y,
                           round(case
                                   when netProfit_TTM_l3y = 0 then
                                    0
                                   when netProfit_TTM_l3y > 0 and
                                        netProfit_TTM_l4y > 0 then
                                    (netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                    netProfit_TTM_l4y
                                   when netProfit_TTM_l3y > 0 and
                                        netProfit_TTM_l4y < 0 then
                                    - ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                       netProfit_TTM_l4y)
                                   when netProfit_TTM_l3y < 0 and
                                        netProfit_TTM_l4y > 0 then
                                    ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                    netProfit_TTM_l4y)
                                   when netProfit_TTM_l3y < 0 and
                                        netProfit_TTM_l4y < 0 then
                                    - ((netProfit_TTM_l3y - netProfit_TTM_l4y) /
                                       netProfit_TTM_l4y)
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
                                   when totalProfit > 0 and totalProfit_yoy > 0 then
                                    (totalProfit - totalProfit_yoy) /
                                    totalProfit_yoy
                                   when totalProfit > 0 and totalProfit_yoy < 0 then
                                    - ((totalProfit - totalProfit_yoy) /
                                       totalProfit_yoy)
                                   when totalProfit < 0 and totalProfit_yoy > 0 then
                                    ((totalProfit - totalProfit_yoy) /
                                    totalProfit_yoy)
                                   when totalProfit < 0 and totalProfit_yoy < 0 then
                                    - ((totalProfit - totalProfit_yoy) /
                                       totalProfit_yoy)
                                 end * 100,
                                 2) as totalProfitinrate,
                           round(case
                                   when totalProfit_l2y = 0 then
                                    0
                                   when totalProfit_yoy > 0 and totalProfit_l2y > 0 then
                                    (totalProfit_yoy - totalProfit_l2y) /
                                    totalProfit_l2y
                                   when totalProfit_yoy > 0 and totalProfit_l2y < 0 then
                                    - ((totalProfit_yoy - totalProfit_l2y) /
                                       totalProfit_l2y)
                                   when totalProfit_yoy < 0 and totalProfit_l2y > 0 then
                                    ((totalProfit_yoy - totalProfit_l2y) /
                                    totalProfit_l2y)
                                   when totalProfit_yoy < 0 and totalProfit_l2y < 0 then
                                    - ((totalProfit_yoy - totalProfit_l2y) /
                                       totalProfit_l2y)
                                 end * 100,
                                 2) as totalProfitinrate_yoy,
                           round(case
                                   when totalProfit_l3y = 0 then
                                    0
                                   when totalProfit_l2y > 0 and totalProfit_l3y > 0 then
                                    (totalProfit_l2y - totalProfit_l3y) /
                                    totalProfit_l3y
                                   when totalProfit_l2y > 0 and totalProfit_l3y < 0 then
                                    - ((totalProfit_l2y - totalProfit_l3y) /
                                       totalProfit_l3y)
                                   when totalProfit_l2y < 0 and totalProfit_l3y > 0 then
                                    ((totalProfit_l2y - totalProfit_l3y) /
                                    totalProfit_l3y)
                                   when totalProfit_l2y < 0 and totalProfit_l3y < 0 then
                                    - ((totalProfit_l2y - totalProfit_l3y) /
                                       totalProfit_l3y)
                                 end * 100,
                                 2) as totalProfitinrate_l2y,
                           round(case
                                   when totalProfit_l4y = 0 then
                                    0
                                   when totalProfit_l3y > 0 and totalProfit_l4y > 0 then
                                    (totalProfit_l3y - totalProfit_l4y) /
                                    totalProfit_l4y
                                   when totalProfit_l3y > 0 and totalProfit_l4y < 0 then
                                    - ((totalProfit_l3y - totalProfit_l4y) /
                                       totalProfit_l4y)
                                   when totalProfit_l3y < 0 and totalProfit_l4y > 0 then
                                    ((totalProfit_l3y - totalProfit_l4y) /
                                    totalProfit_l4y)
                                   when totalProfit_l3y < 0 and totalProfit_l4y < 0 then
                                    - ((totalProfit_l3y - totalProfit_l4y) /
                                       totalProfit_l4y)
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
                                   when NETPROFIT_TTM_yoy <= 0 or
                                        NETPROFIT_TTM = NETPROFIT_TTM_yoy then
                                    0
                                   else
                                    pe_ttm /
                                    (NETPROFIT_TTM / NETPROFIT_TTM_yoy - 1) / 100
                                 end * 100,
                                 2) as PEG,
                           round(case
                                   when OPERATINGREVENUE_yoy <= 0 or
                                        OPERATINGREVENUE = OPERATINGREVENUE_yoy then
                                    0
                                   else
                                    pe_ttm /
                                    (OPERATINGREVENUE / OPERATINGREVENUE_yoy - 1) / 100
                                 end * 100,
                                 2) as PSG,
                           round(case
                                   when totalassets_yoy = 0 or
                                        totalassets = totalassets_yoy then
                                    0
                                   else
                                    pe_ttm / (totalassets / totalassets_yoy - 1) / 100
                                 end * 100,
                                 2) as PBG
                      from stock_analysis_data a
                     where order_date >= to_date('{from_}', 'yyyy-mm-dd')
                       and order_date <= to_date('{to_}', 'yyyy-mm-dd')) b
            on a.code = b.code) h
 where code is not null
'''


def QA_Sql_BlockAnalystic(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch BlockAnalystic QuantData ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)

    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data)

def QA_Sql_BlockAnalysticS(from_ , to_, sql_text = sql_text1, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch BlockAnalysticS QuantData ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)

    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data)