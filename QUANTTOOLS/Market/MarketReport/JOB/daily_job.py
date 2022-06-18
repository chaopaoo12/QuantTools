
from QUANTTOOLS.Ananlysis.Trends.Cbond import CBond
from QUANTTOOLS.Ananlysis.Trends.setting import BTC, GOLD, MONEY, CN_INDEX, US_INDEX, FUTURE, GLOBAL
from QUANTTOOLS.Ananlysis.Trends.trends import btc_daily, money_daily, gold_daily, stock_daily, stock_hourly, future_daily, globalindex_daily
import pandas as pd
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from QUANTTOOLS.Market.MarketTools import predict_base, predict_index_base, predict_index_dev, predict_stock_dev,base_report, load_data

def aotu_report(trading_date):
    BTC_RES = pd.DataFrame(columns=('code', 'daily', 'weekly'))
    #for code in BTC:
    #    res = btc_daily(code)
    #    BTC_RES = BTC_RES.append({'code':code,
    #                              'daily':res[0], 'weekly':res[1],
    #                              'SKDJ_K':res[6], 'SKDJ_TR':res[7],
    #                              'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
    #                              '日线斜率':res[2], '斜率变动':res[3],
    #                              '五日偏离':res[4], '十五日偏离':res[5]}, ignore_index=True)

    for code in GOLD:
        res = gold_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)
    for code in FUTURE:
        res = future_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)

    for code in MONEY:
        res = money_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)

    for code in CN_INDEX:
        res = stock_daily(code, trading_date, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)

    for code in US_INDEX:
        res = stock_daily(code, trading_date, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)

    for code in GLOBAL:
        res = globalindex_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code,
                                  'daily':res[0], 'weekly':res[1],
                                  'SKDJ_K':res[6], 'SKDJ_TR':res[7],
                                  'SKDJ_K_WK':res[8],'SKDJ_TR_WK':res[9],
                                  '日线斜率':res[2], '斜率变动':res[3],
                                  '五日偏离':res[4], '十五日偏离':res[5],
                                  '平均价格水平':res[10], '25%价格水平':res[11],
                                  '75%价格水平':res[12], '目前价格分位数':res[13]}, ignore_index=True)

    #BTC_RES = BTC_RES.rename(columns={'code':'标的', 'daily':'日线走势', 'weekly':'周线走势'}, inplace = True)
    target_body = build_table(BTC_RES[['code','daily','weekly',
                                       'SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK',
                                       '日线斜率','斜率变动','五日偏离','十五日偏离',
                                       '平均价格水平','25%价格水平','75%价格水平','目前价格分位数'
                                       ]], '市场价格监控')
    msg = build_email(build_head(),target_body)
    send_email('金融产品价格趋势' + trading_date, msg, trading_date)
    return(BTC_RES)

def aotu_bond(trading_date):
    df = CBond(trading_date)

    base_report(trading_date, '可转债跟踪', **{'可转债排名':df[df.RANK <= 5][['stock_50','收盘价', '纯债价值', '转股价值', '纯债溢价率', '转股溢价率', '转债名称', '正股名称', '转股价', '回售触发价',
                                                                    '强赎触发价', '到期赎回价', '开始转股日', '上市日期', 'bond_gap', 'stock_gap', 'RRNG',
                                                                    'RRNG_HR', 'PASS_MARK', 'TARGET', 'TARGET3', 'TARGET4', 'TARGET5',
                                                                    'TARGET10', 'RANK']]
                                         })