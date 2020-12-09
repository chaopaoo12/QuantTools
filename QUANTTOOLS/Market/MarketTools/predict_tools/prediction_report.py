
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTAXIS.QAUtil import (QA_util_log_info)

from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from QUANTTOOLS.Trader import get_Client,check_Client

import pandas as pd

def Funding_Decision(trading_date, target_pool, sub_accounts, frozen, percent):
    QA_util_log_info('##JOB## Now Funding Decision ==== {}'.format(str(trading_date)))
    try:
        target = target_pool.loc[trading_date]
    except:
        target = None

    if target is None:
        target = None
    else:
        target = target[['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']].reset_index()
        target = target.assign(close= target['code'].apply(lambda x:float(QA_fetch_stock_day_adv(str(x),trading_date,trading_date).data['close'].values))).set_index('code')

        avg_account = (sub_accounts - frozen)/target.shape[0]
        target = target.assign(tar=avg_account*percent)
        target['cnt'] = (target['tar']/target['close']/100).apply(lambda x:round(x,0)*100)
        target['real'] = target['cnt'] * target['close']
    return(target)

def Current_Report(trading_date, target_pool, top_num=5):
    QA_util_log_info('##JOB## Now Current Report ==== {}'.format(str(trading_date)))
    if target_pool is not None and target_pool.shape[0] > 0:
        if top_num is None or top_num == 0:
            current_score = target_pool[target_pool.OPEN_MARK == 0].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
            current_details = target_pool[['NAME','INDUSTRY','Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','OPEN_MARK','PASS_MARK']]
        else:
            current_score = target_pool[(target_pool['RANK'] <= top_num) & (target_pool.OPEN_MARK == 0)].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
            current_details = target_pool[(target_pool['RANK'] <= top_num) & (target_pool.OPEN_MARK == 0)][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','OPEN_MARK','PASS_MARK']]
    else:
        current_score = pd.DataFrame()
        current_details = pd.DataFrame()

    return(current_score, current_details)

def prediction_report(trading_date, target_pool, prediction, model_date, top_num, exceptions, percent, account='name:client-1',  ui_log = None):
    QA_util_log_info('##JOB## Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, "prediction_report", trading_date, exceptions=exceptions)

    ###预测为正TOP
    current_score, current_details = Current_Report(trading_date, target_pool, top_num)

    ###预测为正T
    currentall_score, currentall_details = Current_Report(trading_date, target_pool)

    ###预测TOP
    top_score, top_details = Current_Report(trading_date, prediction, top_num)

    ####预测的最终结果
    target_fd = Funding_Decision(trading_date, current_details, sub_accounts, frozen, percent)

    QA_util_log_info('##JOB## Now Message Building ==== {}'.format(str(trading_date)), ui_log)

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    try:
        target_body = build_table(target_fd, '目标持仓')
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
        model_score = build_table(current_score, '模型周期内交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内交易成绩", trading_date)

    try:
        stock_socre = build_table(currentall_score, '选股模型周期内交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内交易成绩", trading_date)

    #########
    try:
        modelhis_body = build_table(current_details, '模型周期内选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)

    try:
        allstock_socre = build_table(top_score, '选股模型周期内TOP交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内TOP交易成绩", trading_date)

    try:
        modeltophis_body = build_table(top_details, '模型周期内TOP选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)


    if target_fd is not None:
        title = '交易报告'
    else:
        title = '空仓交易报告'

    try:
        msg = build_email(build_head(),err_msg,
                          target_body,hold_body,fronzen_body,
                          model_score,  stock_socre, allstock_socre,
                          modelhis_body, modeltophis_body)
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
        send_email(title + trading_date, msg, trading_date)
    except:
        send_email('交易报告:'+ trading_date, "消息构建失败", trading_date)
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕 Email过程失败',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    return(target_fd)

def Index_Reporter(trading_date, target_pool, top_num=5):
    QA_util_log_info('##JOB## Now Current Report ==== {}'.format(str(trading_date)))
    if target_pool is not None and target_pool.shape[0] > 0:
        if top_num is None or top_num == 0:
            current_score = target_pool[target_pool.OPEN_MARK == 0].groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]
            current_details = target_pool[['NAME','Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]
        else:
            current_score = target_pool[(target_pool['RANK'] <= top_num) & (target_pool.OPEN_MARK == 0)].groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]
            current_details = target_pool[(target_pool['RANK'] <= top_num) & (target_pool.OPEN_MARK == 0)][['NAME','Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]
    else:
        current_score = pd.DataFrame()
        current_details = pd.DataFrame()

    return(current_score, current_details)

def Index_Report(trading_date, target_pool, prediction, model_date, top_num,  ui_log = None):
    QA_util_log_info('##JOB## Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)

    ###预测为正TOP
    current_score, current_details = Index_Reporter(trading_date, target_pool, top_num)

    ###预测为正T
    currentall_score, currentall_details = Index_Reporter(trading_date, target_pool)

    ###预测TOP
    top_score, top_details = Index_Reporter(trading_date, prediction, top_num)

    ####预测的最终结果
    target_fd = current_details.loc[trading_date]

    QA_util_log_info('##JOB## Now Message Building ==== {}'.format(str(trading_date)), ui_log)

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    try:
        target_body = build_table(target_fd, '目标持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目标持仓", trading_date)

    #########
    try:
        model_score = build_table(current_score, '模型周期内交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内交易成绩", trading_date)

    try:
        stock_socre = build_table(currentall_score, '选股模型周期内交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内交易成绩", trading_date)

    #########
    try:
        modelhis_body = build_table(current_details, '模型周期内选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)

    try:
        allstock_socre = build_table(top_score, '选股模型周期内TOP交易成绩')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:选股模型周期内TOP交易成绩", trading_date)

    try:
        modeltophis_body = build_table(top_details, '模型周期内TOP选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:模型周期内选股记录", trading_date)


    if target_fd is not None:
        title = '交易报告'
    else:
        title = '空仓交易报告'

    try:
        msg = build_email(build_head(),err_msg,
                          target_body,
                          model_score,  stock_socre, allstock_socre,
                          modelhis_body, modeltophis_body)
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
        send_email(title + trading_date, msg, trading_date)
    except:
        send_email('交易报告:'+ trading_date, "消息构建失败", trading_date)
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕 Email过程失败',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    return(target_fd)
