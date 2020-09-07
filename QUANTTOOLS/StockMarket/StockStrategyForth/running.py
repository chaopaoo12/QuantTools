
from QUANTTOOLS.StockMarket.StockStrategyForth.concat_predict import concat_predict,save_prediction
from QUANTTOOLS.StockMarket.StockStrategyForth.setting import working_dir, percent, exceptions

from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_day
from QUANTAXIS.QAUtil import (QA_util_log_info)

from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.account_manage import get_Client,check_Client
from QUANTTOOLS.message_func.wechat import send_actionnotice

import pandas as pd
from datetime import timedelta
delta3 = timedelta(days=7)

def predict(trading_date, strategy_id='机器学习1号', account='name:client-1', working_dir=working_dir, ui_log = None, exceptions=exceptions):

    QA_util_log_info('##JOB01 Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    QA_util_log_info('##JOB02 Now Predict ==== {}'.format(str(trading_date)), ui_log)
    tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    QA_util_log_info('##JOB03 Now Saving Result ==== {}'.format(str(trading_date)), ui_log)
    save_prediction({'date': trading_date, 'tar':tar}, 'prediction', working_dir)

    QA_util_log_info('##JOB04 Now Funding Decision ==== {}'.format(str(trading_date)), ui_log)

    #try:
    #    index1 = tar_index.loc[trading_date][['NAME','Z_PROB','O_PROB','RANK','model_type']]
    #except:
    #    index1 = None

    try:
        tar1 = tar.loc[trading_date]
    except:
        tar1 = None

    if tar1 is None:
        res = None
    else:
        tar2 = tar1[['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']].reset_index()

        tar2 = tar2.assign(close= tar2['code'].apply(lambda x:QA_fetch_stock_day(str(x),trading_date,trading_date)['close']))
        res = tar2.set_index('code')

        avg_account = (sub_accounts - frozen)/tar1.shape[0]
        res = res.assign(tar=avg_account*percent)
        res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
        res['real'] = res['cnt'] * res['close']

    QA_util_log_info('##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)
    #table1 = tar[tar['RANK']<=5].groupby('date').mean()
    if tar is not None and tar.shape[0] > 0:
        table1 = tar.groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
    else:
        table1 = pd.DataFrame()
        tar = pd.DataFrame()

    QA_util_log_info('##JOB06 Now Message Building ==== {}'.format(str(trading_date)), ui_log)
    #try:
    #    safe_res = safe_tar.loc[trading_date][['NAME','Z_PROB','O_PROB','RANK']]
    #except:
    #    safe_res = pd.DataFrame()

    #try:
    #    index_res = index_tar.loc[trading_date][['NAME','Z_PROB','O_PROB','RANK']]
    #except:
    #    index_res = pd.DataFrame()

    try:
        stock_res = stock_tar[stock_tar['RANK']<=5].loc[trading_date][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
    except:
        stock_res = pd.DataFrame()

    #index_d = index_tar.groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
    stock_d = stock_tar.groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
    #combine_d = tar_index.groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    ########

    #try:
    #    safe_body = build_table(safe_res, 'safe模型结果_{}'.format(str(trading_date)))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:Safe模型结果", trading_date)

    #try:
    #    index_body = build_table(index_res, '指数模型结果_{}'.format(str(trading_date)))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:指数模型结果", trading_date)

    try:
        stock_body = build_table(stock_res, '选股模型结果_{}'.format(str(trading_date)))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型结果", trading_date)

    #try:
    #    combine_body = build_table(index1, '合成指数模型结果_from:{a}_to:{b}'.format(a=start, b=end))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:合成指数模型结果", trading_date)

    #########

    try:
        target_body = build_table(res, '目标持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目标持仓", trading_date)

    try:
        hold_body = build_table(positions, '目前持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前持仓", trading_date)

    try:
        fronzen_body = build_table(frozen_positions, '目前锁定持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前锁定持仓", trading_date)

    #########
    try:
        model_score = build_table(table1, '模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内交易成绩", trading_date)

    #try:
    #    combine_score = build_table(combine_d, '合成指数模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:合成指数模型模型周期内交易成绩", trading_date)

    #try:
    #    index_score = build_table(index_d, '基础指数模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:基础指数模型周期内交易成绩", trading_date)

    try:
        stock_socre = build_table(stock_d, '选股模型周期内交易成绩_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内交易成绩", trading_date)

    #########

    try:
        modelhis_body = build_table(tar[['NAME','INDUSTRY','Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']], '模型周期内选股记录_from:{a}_to:{b}'.format(a=start, b=end))
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)

    #try:
    #    indexhis_body = build_table(tar_index[['NAME','Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']], '模型周期内指数合成记录_from:{a}_to:{b}'.format(a=start, b=end))
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内指数合成记录", trading_date)


    if res is not None:
        title = '交易报告'
    else:
        title = '空仓交易报告'

    try:
        msg = build_email(build_head(),err_msg,stock_body,
                          target_body,hold_body,fronzen_body,
                          model_score,  stock_socre,
                          modelhis_body)
        send_actionnotice(strategy_id,
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
        send_email(title + trading_date, msg, trading_date)
    except:
        send_email('交易报告:'+ trading_date, "消息构建失败", trading_date)
        send_actionnotice(strategy_id,
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕 Email过程失败',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    return(tar)




