from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv,QA_fetch_index_list_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.concat_predict import concat_predict,save_prediction
import pandas as pd
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTAXIS.QAUtil import QA_util_get_last_day
from QUANTTOOLS.account_manage import get_Client,check_Client
from datetime import timedelta
delta3 = timedelta(days=7)

def predict(trading_date, strategy_id='机器学习1号', account='name:client-1', working_dir=working_dir, ui_log = None, exceptions=exceptions):

    QA_util_log_info(
        '##JOB01 Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    QA_util_log_info('##JOB02 Now Predict ==== {}'.format(str(trading_date)), ui_log)
    tar,index_tar,safe_tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'tar':tar}, 'prediction', working_dir)

    QA_util_log_info('##JOB03 Now Concat Result ==== {}'.format(str(trading_date)), ui_log)
    info = QA_fetch_stock_fianacial_adv(list(set(tar.reset_index('date').index)), trading_date, trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
    tar = tar.reset_index('date').join(info, how = 'left').reset_index().set_index(['date','code']).sort_index()

    QA_util_log_info(
        '##JOB04 Now Funding Decision ==== {}'.format(str(trading_date)), ui_log)
    try:
        tar1 = tar.loc[trading_date]
    except:
        tar1 = None

    if tar1 is None:
        res = None
    else:
        tar2 = tar1[['Z_PROB','O_PROB','RANK']]
        close = QA_fetch_stock_day_adv(list(tar1.index),QA_util_get_last_day(trading_date,60),trading_date).to_qfq().data.loc[trading_date].reset_index('date')['close']
        res = tar2.join(close, how = 'left')
        avg_account = sub_accounts['总 资 产']/tar1.shape[0]
        res = res.assign(tar=avg_account[0]*percent)
        res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
        res['real'] = res['cnt'] * res['close']

    QA_util_log_info(
        '##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)
    #table1 = tar[tar['RANK']<=5].groupby('date').mean()
    if tar is not None and tar.shape[0] > 0:
        table1 = tar.groupby('date').mean()
    else:
        table1 = pd.DataFrame()
        tar = pd.DataFrame()

    QA_util_log_info(
        '##JOB06 Now Message Building ==== {}'.format(str(trading_date)), ui_log)
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

    index_d = index_tar.groupby('date').mean()
    stock_d = stock_tar.groupby('date').mean()

    try:
        msg1 = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    try:
        body1 = build_table(safe_res, 'safe模型结果_{}'.format(str(trading_date)))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Safe模型结果", trading_date)

    try:
        body3 = build_table(positions, '目前持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前持仓", trading_date)

    try:
        body4 = build_table(index_res, '指数模型结果_{}'.format(str(trading_date)))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:指数模型结果", trading_date)

    try:
        body5 = build_table(stock_res, '选股模型结果_{}'.format(str(trading_date)))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型结果", trading_date)

    try:
        body7 = build_table(frozen_positions, '目前锁定持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前锁定持仓", trading_date)

    try:
        body8 = build_table(tar, '模型周期内选股记录_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)

    try:
        body9 = build_table(table1, '模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内交易成绩", trading_date)

    try:
        body10 = build_table(index_d, '指数模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:指数模型周期内交易成绩", trading_date)

    try:
        body11 = build_table(stock_d, '选股模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内交易成绩", trading_date)

    try:
        body2 = build_table(res, '目标持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目标持仓", trading_date)

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




