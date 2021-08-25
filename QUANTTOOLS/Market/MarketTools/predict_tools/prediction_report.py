
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTAXIS.QAUtil import (QA_util_log_info)

from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from QUANTTOOLS.Trader import get_Client,check_Client

import pandas as pd

def Current_Report(trading_date, target_pool, top_num=5, name_list = None, value_ist = None, sort_mark = None):
    QA_util_log_info('##JOB## Now Current Report ==== {}'.format(str(trading_date)))
    if target_pool is not None and target_pool.shape[0] > 0:
        if top_num is None or top_num == 0:
            current_score = target_pool.groupby('date').mean()[value_ist]
            current_details = target_pool[name_list + value_ist]
        else:
            current_score = target_pool[(target_pool[sort_mark] <= top_num)].groupby('date').mean()[value_ist]
            current_details = target_pool[(target_pool[sort_mark] <= top_num)][name_list + value_ist].reset_index().sort_values(by=['date','RANK'],ascending=[True,True]).set_index(['date','code'])
    else:
        current_score = pd.DataFrame()
        current_details = pd.DataFrame()

    return(current_score, current_details)

def Funding_Decision(trading_date, target_pool, sub_accounts, frozen, percent, selec_list = None):
    QA_util_log_info('##JOB## Now Funding Decision ==== {}'.format(str(trading_date)))
    try:
        target = target_pool.loc[trading_date]
    except:
        target = None

    if target is None:
        target = None
    else:
        target = target[selec_list].reset_index()
        target = target.assign(close= target['code'].apply(lambda x:float(QA_fetch_stock_day_adv(str(x),trading_date,trading_date).data['close'].values))).set_index('code')

        avg_account = (sub_accounts - frozen)/target.shape[0]
        target = target.assign(tar=avg_account*percent)
        target['cnt'] = (target['tar']/target['close']/100).apply(lambda x:round(x,0)*100)
        target['real'] = target['cnt'] * target['close']
    return(target)


def prediction_report(trading_date, target_pool, prediction, model_date, top_num, exceptions, percent,
                      name_list = ['NAME','INDUSTRY'],
                      value_ist = ['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK'],
                      sort_mark ='RANK',
                      selec_list=['NAME','INDUSTRY','Z_PROB','O_PROB','RANK'],
                      account='name:client-1',  ui_log = None):
    QA_util_log_info('##JOB## Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, "prediction_report", trading_date, exceptions=exceptions)

    ###XG股池 + 交易成绩
    current_score, current_details = Current_Report(trading_date, target_pool, top_num, name_list = name_list, value_ist = value_ist, sort_mark =sort_mark)

    ###XG+DAY_LINE
    currentall_score, currentall_details = Current_Report(trading_date, target_pool, name_list = name_list, value_ist = value_ist, sort_mark =sort_mark)

    ###XG+DAY_LINE预测TOP
    top_score, top_details = Current_Report(trading_date, prediction, top_num, name_list = name_list, value_ist = value_ist, sort_mark =sort_mark)

    ####预测的最终结果
    target_fd = Funding_Decision(trading_date, current_details, sub_accounts, frozen, percent, selec_list = selec_list)

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
            current_details = target_pool[['NAME','SKDJ_K','SKDJ_K_HR','DAY_PROB','DAY_RANK','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
        else:
            current_details = target_pool[target_pool['DAY_RANK'] <= top_num][['NAME','SKDJ_K','SKDJ_K_HR','DAY_PROB','DAY_RANK','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
    else:
        current_details = pd.DataFrame()

    return(current_details)

def Index_Report(trading_date, prediction, hour_prediction, model_date):
    QA_util_log_info('##JOB## Now Got Account Info ==== {}'.format(str(trading_date)))

    ###XG选股模型
    #Rank_index = prediction[(prediction.DAY_RANK <= 5)][['NAME','DAY_RANK','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']].reset_index().sort_values(by=['date','SKDJ_K_WK'],ascending=[False,True]).set_index(['date','code'])

    ###目前趋势中的指数
    terns_index = prediction[(prediction.DAY_RANK <= 10)].loc[trading_date][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB']].sort_values('SKDJ_K',ascending=True)

    ###周线级别机会
    WK_index = prediction[(prediction.SKDJ_K_WK <= 30)][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']].reset_index().sort_values(by=['date','SKDJ_K_WK'],ascending=[False,True]).set_index(['date','code'])

    ###日线级别机会
    DAY_index = prediction[(prediction.SKDJ_K <= 30)][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']].reset_index().sort_values(by=['date','SKDJ_K'],ascending=[False,True]).set_index(['date','code'])

    ###小时线级别机会
    HR_index = prediction[(prediction.SKDJ_K_HR <= 30)][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']].reset_index().sort_values(by=['date','SKDJ_K_HR'],ascending=[False,True]).set_index(['date','code'])

    ###日线入场信号
    #try:
    #    in_list = prediction[((prediction.SKDJ_CROSS2 == 1) | (prediction.CROSS_JC == 1))& (prediction.CCI > 0)].loc[trading_date][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB']]
    #except:
    #    in_list = None

    #try:
    #    out_list = prediction[(prediction.SKDJ_CROSS1 == 1)].loc[trading_date][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB']]
    #except:
    #    out_list = None

    ###hour线入场信号
    #try:
    #    in_list_HR = hour_prediction[((hour_prediction.SKDJ_CROSS2_HR == 1) | (hour_prediction.CROSS_JC_HR == 1))& (hour_prediction.CCI_HR > 0)].loc[trading_date]
    #except:
    #    in_list_HR = None

    #try:
    #    out_list_HR = hour_prediction[(hour_prediction.SKDJ_CROSS1_HR == 1)].loc[trading_date]
    #except:
    #    out_list_HR = None

    ###近期表现强势的指数
    try:
        top_index = prediction[prediction.INDEX_TARGET5 > 5][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','DAY_RANK','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
    except:
        top_index = None

    ####近期选股记录
    #hour_prediction['SHIFT_O_PROB'] = hour_prediction['O_PROB'].groupby('code').shift()

    try:
        target_fd = prediction[(prediction.DAY_RANK <= 10)][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','RSI3','RSI2','RSI3_C','RSI2_C','DAY_RANK','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']].reset_index().sort_values(by=['date','DAY_RANK'],ascending=[False,True]).set_index(['date','code'])
    except:
        target_fd = None

    try:
        target_hr = hour_prediction[((hour_prediction.SKDJ_CROSS2_HR == 1) | (hour_prediction.CROSS_JC_HR == 1))& (hour_prediction.CCI_HR > 0)]
    except:
        target_hr = None
    ###小时级趋势延续至日线 不需要

    ####大盘情况预测
    market_000001 = prediction.loc[(slice(None),'000001'),][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5','RSI3','RSI2','RSI3_C','RSI2_C']]
    market_399001 = prediction.loc[(slice(None),'399001'),][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5','RSI3','RSI2','RSI3_C','RSI2_C']]
    market_399006 = prediction.loc[(slice(None),'399006'),][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5','RSI3','RSI2','RSI3_C','RSI2_C']]
    market_399005 = prediction.loc[(slice(None),'399005'),][['NAME','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','DAY_PROB','HOUR_PROB','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5','RSI3','RSI2','RSI3_C','RSI2_C']]

    QA_util_log_info('##JOB## Now Message Building ==== {}'.format(str(trading_date)))

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    try:
        terns_body = build_table(terns_index, '目前趋势中的指数')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前趋势中的指数", trading_date)

    try:
        WK_body = build_table(WK_index, '周线级别机会')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:周线级别机会", trading_date)

    try:
        DAY_body = build_table(DAY_index, '日线级别机会')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:日线级别机会", trading_date)

    try:
        HR_body = build_table(HR_index, '小时级别机会')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:小时级别机会", trading_date)

    try:
        market_000001 = build_table(market_000001, '上证指数情况')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:上证指数情况", trading_date)

    try:
        market_399001 = build_table(market_399001, '深证指数情况')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:深证指数情况", trading_date)

    try:
        market_399005 = build_table(market_399005, '中小板指数情况')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:中小板指数情况", trading_date)

    try:
        market_399006 = build_table(market_399006, '创业指数情况')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:创业指数情况", trading_date)

    #########
    #try:
    #    in_body = build_table(in_list, '入场提示')
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:入场提示", trading_date)

    #try:
    #    out_body = build_table(out_list, '出场提示')
    #except:
    #    send_email('交易报告:'+ trading_date, "消息组件运算失败:出场提示", trading_date)

    try:
        terns_his_body = build_table(top_index, '近期表现强势的指数')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:近期表现强势的指数", trading_date)

    try:
        terns_h_body = build_table(target_hr, '近期hour选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:近期hour选股记录", trading_date)

    try:
        terns_t_body = build_table(target_fd, '近期选股记录')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:近期选股记录", trading_date)

    title = '市场状态报告'


    try:
        msg = build_email(build_head(),err_msg,
                          terns_body,#in_body,out_body,
                          market_000001,market_399001,market_399005, market_399006,
                          WK_body,DAY_body,HR_body,
                          terns_his_body, terns_t_body, terns_h_body)
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


def base_report(trading_date, title, **kwargs):

    QA_util_log_info('##JOB## Now Message Building ==== {}'.format(str(trading_date)))

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=trading_date)
    except:
        err_msg = 'error'
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    bodys = ''

    for i in kwargs:
        try:
            body = build_table(kwargs[i], '{}周期内选股记录'.format(i))
            bodys = bodys + body
        except:
            send_email('交易报告:'+ trading_date, "消息组件运算失败:{}周期内选股记录".format(i), trading_date)

    try:
        msg = build_email(build_head(), err_msg, bodys
                          )
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
        send_email(title + trading_date, msg, trading_date)
    except:
        send_email(title + trading_date, "消息构建失败", trading_date)
        send_actionnotice("prediction_report",
                          '交易报告:{}'.format(trading_date),
                          '模型运行完毕 Email过程失败',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    return(None)

if __name__ == '__main__':
    pass