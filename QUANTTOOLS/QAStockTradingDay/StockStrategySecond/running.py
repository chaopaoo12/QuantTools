
import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyOne as Stock
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne as Index
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv,QA_fetch_index_list_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
from QUANTTOOLS.FactorTools.base_tools import combine_model
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTAXIS.QAUtil import QA_util_get_last_day
from QUANTTOOLS.account_manage import get_Client
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *
delta3 = timedelta(days=7)

def predict(trading_date, strategy_id='机器学习1号', account1='name:client-1', working_dir=working_dir, ui_log = None, exceptions=exceptions):

    try:
        QA_util_log_info(
            '##JOB01 Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
        client = get_Client()
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
        try:
            frozen = float(client.get_positions(account1)['positions'].set_index('证券代码').loc[exceptions]['市值'].sum())
        except:
            frozen = 0
        sub_accounts = sub_accounts - frozen
    except:
        send_email('错误报告', '云服务器错误,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    try:
        QA_util_log_info(
            '##JOB02 Now Load Model ==== {}'.format(str(trading_date)), ui_log)
        stock_model_temp,stock_info_temp = Stock.load_model('stock',working_dir = working_dir)
        index_model_temp,index_info_temp = Index.load_model('index',working_dir = working_dir)
        safe_model_temp,safe_info_temp = Index.load_model('safe',working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    start = (datetime.strptime(trading_date, "%Y-%m-%d") + relativedelta(weekday=FR(-1))).strftime('%Y-%m-%d')
    end = trading_date
    rng = pd.Series(pd.date_range(start, end, freq='D')).apply(lambda x: str(x)[0:10])
    print(start, end)
    QA_util_log_info(
        '##JOB03 Now Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #index_list,index_report,index_top_report = Index.check_model(index_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),index_info_temp['cols'], 'INDEXT_TARGET5', 0.3)
    index_tar,index_b  = Index.model_predict(index_model_temp, start, end, index_info_temp['cols'])

    #safe_list,safe_report,safe_top_report = Index.check_model(safe_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),safe_info_temp['cols'], 'INDEXT_TARGET', 0.3)
    safe_tar,safe_b  = Index.model_predict(safe_model_temp, start, end, index_info_temp['cols'])

    #stock_list,report,top_report = Stock.check_model(stock_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),stock_info_temp['cols'], 0.42)
    stock_tar,stock_b  = Stock.model_predict(stock_model_temp, start, end, stock_info_temp['cols'])

    tar = combine_model(index_b, stock_b, safe_b, start, trading_date)
    QA_util_log_info(
        '##JOB03 Now Concat Result ==== {}'.format(str(trading_date)), ui_log)
    try:
        tar1 = tar.loc[trading_date]
    except:
        tar1 = None

    QA_util_log_info(
        '##JOB04 Now Funding Decision ==== {}'.format(str(trading_date)), ui_log)
    if tar1 is None:
        res = None
    else:
        tar2 = tar1[['Z_PROB','O_PROB','RANK']]
        close = QA_fetch_stock_day_adv(list(tar2.index),QA_util_get_last_day(trading_date,60),trading_date).to_qfq().data.loc[trading_date].reset_index('date')['close']
        info = QA_fetch_stock_fianacial_adv(list(tar1.index), trading_date, trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
        res = tar2.join(close, how = 'left').join(info, how = 'left')
        #res = pd.concat([tar2,close,info],axis=1)
        avg_account = sub_accounts['总 资 产']/tar1.shape[0]
        res = res.assign(tar=avg_account[0]*percent)
        res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
        res['real'] = res['cnt'] * res['close']

    QA_util_log_info(
        '##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)
    #table1 = tar[tar['RANK']<=5].groupby('date').mean()
    print(tar)
    info1 = QA_fetch_stock_fianacial_adv(list(set(tar.reset_index('date').index)), trading_date, trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
    tar = tar.reset_index('date').join(info1, how = 'left').reset_index().set_index(['date','code']).sort_index()
    if exceptions is not None:
        frozen_positions = client.get_positions(account1)['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']].set_index('证券代码').loc[exceptions]
    else:
        frozen_positions = pd.DataFrame()

    QA_util_log_info(
        '##JOB06 Now Current Holding ==== {}'.format(str(trading_date)), ui_log)
    positions = client.get_positions(account1)['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']]

    QA_util_log_info(
        '##JOB07 Now Message Building ==== {}'.format(str(trading_date)), ui_log)
    try:
        safe_res = safe_tar.loc[trading_date]
        info1 = QA_fetch_index_list_adv().loc[list(set(safe_res.index))][['name']]
        safe_res = safe_res.join(info1, how = 'left').sort_index()
    except:
        safe_res = pd.DataFrame()

    try:
        index_res = index_tar.loc[trading_date]
        info1 = QA_fetch_index_list_adv().loc[list(set(index_res.index))][['name']]
        index_res = index_res.join(info1, how = 'left').sort_index()
    except:
        index_res = pd.DataFrame()

    try:
        stock_res = stock_tar[stock_tar['RANK']<=5].loc[trading_date]
        info1 = QA_fetch_stock_fianacial_adv(list(set(stock_res.index)), trading_date, trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
        stock_res = stock_res.join(info1, how = 'left').sort_index()
    except:
        stock_res = pd.DataFrame()

    if len(rng) == 1:
        table1 = tar.mean()
        index_d = index_tar.mean()
        stock_d = stock_tar.mean()
    else:
        table1 = tar.groupby('date').mean()
        index_d = index_tar.groupby('date').mean()
        stock_d = stock_tar.groupby('date').mean()

    try:
        msg1 = '模型训练日期:{model_date}'.format(model_date=stock_info_temp['date'])
        body1 = build_table(safe_res, 'safe模型结果_{}'.format(str(QA_util_get_last_day(trading_date))))
        body3 = build_table(positions, '目前持仓')
        body4 = build_table(index_res, '指数模型结果_{}'.format(str(QA_util_get_last_day(trading_date))))
        body5 = build_table(stock_res, '选股模型结果_{}'.format(str(QA_util_get_last_day(trading_date))))
        #body6 = build_table(stock_list, '上一交易日模型交易清单{}'.format(str(QA_util_get_last_day(trading_date))))
        body7 = build_table(frozen_positions, '目前锁定持仓')
        body8 = build_table(tar, '模型周期内选股记录_from:{a}_to:{b}'.format(a=start, b=end))
        body9 = build_table(table1, '模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
        body10 = build_table(index_d, '指数模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
        body11 = build_table(stock_d, '选股模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
        body2 = build_table(res, '目标持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败", trading_date)

    if res is not None:
        title = '交易报告'
    else:
        title = '空仓交易报告'

    try:
        msg = build_email(build_head(),msg1,body1,body4,body5,body3,body2,body7,body8,body9,body10, body11)
        send_email(title + trading_date, msg, trading_date)
    except:
        send_email('交易报告:'+ trading_date, "消息构建失败", trading_date)

    return(tar)




