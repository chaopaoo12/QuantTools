import cx_Oracle
import pandas as pd
import datetime

from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)

def QA_util_process_quantdata(type = 'day', start_date = None, end_date = None):

    if type == 'day' or start_date == None:
        start_date = QA_util_today_str()
        end_date = QA_util_today_str()

    sql1 = """INSERT INTO QUANT_ANALYSIS_DATA
  select *
    from (select A.*,
   LAG(TOTAL_MARKET) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE_MARKET,
   LAG(TOTAL_MARKET, 2) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE2_MARKET,
   LAG(TOTAL_MARKET, 3) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE3_MARKET,
   LAG(TOTAL_MARKET, 5) OVER(PARTITION BY A.CODE ORDER BY ORDER_DATE DESC) AS PRE5_MARKET,
   LAG(AVG_TOTAL_MARKET) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE_MARKET,
   LAG(AVG_TOTAL_MARKET, 2) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE2_MARKET,
   LAG(AVG_TOTAL_MARKET, 3) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE3_MARKET,
   LAG(AVG_TOTAL_MARKET, 5) OVER(PARTITION BY CODE ORDER BY ORDER_DATE DESC) AS AVG_PRE5_MARKET
            from (select *,
                    from stock_analysis_data a where order_date <= (to_date('{deal_date}', 'yyyy-mm-dd') + 5)
           and order_date >= to_date('{deal_date}', 'yyyy-mm-dd')
                    ) A
        where ORDER_DATE = '{deal_date}'"""

    conn = cx_Oracle.connect('quantaxis/123@192.168.3.56:1521/quantaxis')
    cursor = conn.cursor()
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    while start_date <= end_date:
        if QA_util_if_trade(start_date) == True:
            print(start_date.strftime("%Y-%m-%d"), QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5))
            sql2 = sql1.format(deal_date=QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5))
            if QA_util_get_pre_trade_date(start_date.strftime("%Y-%m-%d"),5) == 'wrong date':
                print(sql2)
            cursor.execute(sql2)
            print('quant analysis data for {deal_date} has been stored'.format(deal_date=start_date.strftime("%Y-%m-%d")))
            start_date = start_date+datetime.timedelta(days=1)
            conn.commit()

    cursor.close()
    conn.commit()
    conn.close()

def QA_util_etl_stock_quant(deal_date = None):

    sql = '''select code,
       name,
       industry,
       to_char(order_date, 'yyyy-mm-dd') as "date",
       open,
       high,
       low,
       close,
       vol,
       amount,
       shares,
       total_market,
       tra_total_market,
       tra_total_market / total_market AS tra_rate,
       pe,
       pb,
       roe,
       roe_ly,
       roe_l2y,
       roe_l3y,
       roe_l4y,
       roa,
       roa_ly,
       roa_l2y,
       roa_l3y,
       roa_l4y,
       grossMargin,
       grossMargin_ly,
       grossMargin_l2y,
       grossMargin_l3y,
       grossMargin_l4y,
       case
         when operatingRevenue_TTM_ly = 0 then
          0
         else
          operatingRevenue_TTM / operatingRevenue_TTM_ly - 1
       end AS operatingRinrate,
       case
         when operatingRevenue_TTM_l2y = 0 then
          0
         else
          operatingRevenue_TTM_ly / operatingRevenue_TTM_l2y - 1
       end AS operatingRinrate_ly,
       case
         when operatingRevenue_TTM_l3y = 0 then
          0
         else
          operatingRevenue_TTM_l2y / operatingRevenue_TTM_l3y - 1
       end AS operatingRinrate_l2y,
       case
         when operatingRevenue_TTM_l4y = 0 then
          0
         else
          operatingRevenue_TTM_l3y / operatingRevenue_TTM_l4y - 1
       end AS operatingRinrate_l3y,
       case
         when netProfit_TTM_ly = 0 then
          0
         else
          netProfit_TTM / netProfit_TTM_ly - 1
       end as netProfit_inrate,
       case
         when netProfit_TTM_l2y = 0 then
          0
         else
          netProfit_TTM_ly / netProfit_TTM_l2y - 1
       end as netProfit_inrate_ly,
       case
         when netProfit_TTM_l3y = 0 then
          0
         else
          netProfit_TTM_l2y / netProfit_TTM_l3y - 1
       end as netProfit_inrate_l2y,
       case
         when netProfit_TTM_l4y = 0 then
          0
         else
          netProfit_TTM_l3y / netProfit_TTM_l4y - 1
       end as netProfit_inrate_l3y,
       case
         when netCashOperatActiv_TTM_ly = 0 then
          0
         else
          netCashOperatActiv_TTM / netCashOperatActiv_TTM_ly - 1
       end as netCashOperatinrate,
       case
         when netCashOperatActiv_TTM_l2y = 0 then
          0
         else
          netCashOperatActiv_TTM_ly / netCashOperatActiv_TTM_l2y - 1
       end as netCashOperatinrate_ly,
       case
         when netCashOperatActiv_TTM_l3y = 0 then
          0
         else
          netCashOperatActiv_TTM_l2y / netCashOperatActiv_TTM_l3y - 1
       end as netCashOperatinrate_l2y,
       case
         when netCashOperatActiv_TTM_l4y = 0 then
          0
         else
          netCashOperatActiv_TTM_l3y / netCashOperatActiv_TTM_l4y - 1
       end as netCashOperatinrate_l3y,
       case
         when totalProfit_TTM_ly = 0 then
          0
         else
          totalProfit_TTM / totalProfit_TTM_ly - 1
       end as totalProfitinrate,
       case
         when totalProfit_TTM_l2y = 0 then
          0
         else
          totalProfit_TTM_ly / totalProfit_TTM_l2y - 1
       end as totalProfitinrate_ly,
       case
         when totalProfit_TTM_l3y = 0 then
          0
         else
          totalProfit_TTM_l2y / totalProfit_TTM_l3y - 1
       end as totalProfitinrate_l2y,
       case
         when totalProfit_TTM_l4y = 0 then
          0
         else
          totalProfit_TTM_l3y / totalProfit_TTM_l4y - 1
       end as totalProfitinrate_l3y,
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
       end as PEG,
       case
         when OPERATINGREVENUE_TTM_LY = 0 or
              OPERATINGREVENUE_TTM = OPERATINGREVENUE_TTM_LY then
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
       AVG5_TOR,
       AVG20_TOR,
       AVG30_TOR,
       AVG60_TOR,
       RNG_L,
       RNG_5,
       RNG_20,
       RNG_30,
       RNG_60,
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
             2) as target5
  from QUANT_ANALYSIS_DATA A
 where order_date = to_date('{start_date}', 'yyyy-mm-dd')'''
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
            return(data)