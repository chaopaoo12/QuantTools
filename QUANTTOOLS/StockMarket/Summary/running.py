from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_close
from QUANTTOOLS.StockMarket.StockStrategySecond.concat_predict import concat_predict,save_prediction
import pandas as pd
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.StockMarket.StockStrategyThird.setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.account_manage import get_Client,check_Client
from QUANTTOOLS.message_func.wechat import send_actionnotice
from datetime import timedelta
delta3 = timedelta(days=7)

def predict(trading_date, strategy_id='机器学习1号', account='name:client-1', working_dir=working_dir, ui_log = None, exceptions=exceptions):

    QA_util_log_info('##JOB01 Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    QA_util_log_info('##JOB02 Now Predict ==== {}'.format(str(trading_date)), ui_log)
    stock_xgboost_tar,stock_keras_tar,index_xgboost_tar,index_keras_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    QA_util_log_info('##JOB03 Now Saving Result ==== {}'.format(str(trading_date)), ui_log)
    try:
        stock_res = stock_xgboost_tar[stock_xgboost_tar['RANK']<=5][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
    except:
        stock_res = pd.DataFrame()
    save_prediction({'date': trading_date, 'tar':stock_res}, 'prediction', working_dir)

    QA_util_log_info('##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)

    stock_xgboost_his_top5 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 5][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
    stock_xgboost_top5 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 5].groupby('date').mean()['TARGET'].sum()
    stock_xgboost_score_top5 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 5].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    stock_xgboost_top10 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 10].groupby('date').mean()['TARGET'].sum()
    stock_xgboost_score_top10 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 10].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    stock_xgboost_top20 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 20].groupby('date').mean()['TARGET'].sum()
    stock_xgboost_score_top20 = stock_xgboost_tar[stock_xgboost_tar.RANK <= 20].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    stock_keras_his_top5 = stock_keras_tar[stock_keras_tar.RANK <= 5][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
    stock_keras_top5 = stock_keras_tar[stock_keras_tar.RANK <= 5].groupby('date').mean()['TARGET'].sum()
    stock_keras_score_top5 = stock_keras_tar[stock_keras_tar.RANK <= 5].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    stock_keras_top10 = stock_keras_tar[stock_keras_tar.RANK <= 10].groupby('date').mean()['TARGET'].sum()
    stock_keras_score_top10 = stock_keras_tar[stock_keras_tar.RANK <= 10].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    stock_keras_top20 = stock_keras_tar[stock_keras_tar.RANK <= 20].groupby('date').mean()['TARGET'].sum()
    stock_keras_score_top20 = stock_keras_tar[stock_keras_tar.RANK <= 20].groupby('date').mean()[['Z_PROB','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]

    index_xgboost_his_top5 = index_xgboost_tar[index_xgboost_tar.RANK <= 5][['NAME','Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
    index_xgboost_top5 = index_xgboost_tar[index_xgboost_tar.RANK <= 5].groupby('date').mean()['INDEX_TARGET'].sum()
    index_xgboost_score_top5 = index_xgboost_tar[index_xgboost_tar.RANK <= 5].groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]

    index_keras_his_top5 = index_keras_tar[index_keras_tar.RANK <= 5][['NAME','Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]
    index_keras_top5 = index_keras_tar[index_keras_tar.RANK <= 5].groupby('date').mean()['INDEX_TARGET'].sum()
    index_keras_score_top5 = index_keras_tar[index_keras_tar.RANK <= 5].groupby('date').mean()[['Z_PROB','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5']]

    QA_util_log_info('##JOB06 Now Message Building ==== {}'.format(str(trading_date)), ui_log)

    if stock_xgboost_tar[stock_xgboost_tar.RANK <= 5].loc[trading_date] is not None:
        title = '交易报告'
    else:
        title = '空仓交易报告'

    try:
        err_msg = '模型训练日期:{model_date}'.format(model_date=model_date)
    except:
        send_email('交易报告:'+ trading_date, "模型训练日期获取运算失败", trading_date)

    try:
        top5_summary_msg = '最近TOP5交易成绩总览: {sx_top5} {sk_top5} {ix_top5} {ik_top5}'.format(
            sx_top5=stock_xgboost_top5,sk_top5=stock_keras_top5,ix_top5=index_xgboost_top5,ik_top5=index_keras_top5)
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:TOP5交易成绩总览", trading_date)

    try:
        top10_summary_msg = '最近TOP10交易成绩总览: {sx_top10} {sk_top10}'.format(
            sx_top10=stock_xgboost_top10,sk_top10=stock_keras_top10)
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:TOP10交易成绩总览", trading_date)

    try:
        top20_summary_msg = '最近TOP20交易成绩总览: {sx_top20} {sk_top20}'.format(
            sx_top20=stock_xgboost_top20,sk_top20=stock_keras_top20)
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:TOP20交易成绩总览", trading_date)

    try:
        sx_top5_msg = build_table(stock_xgboost_score_top5, 'Stock XGBoost TOP5交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock XGBoost TOP5交易成绩清单", trading_date)

    try:
        sx_top10_msg = build_table(stock_xgboost_score_top10, 'Stock XGBoost TOP10交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock XGBoost TOP10交易成绩清单", trading_date)

    try:
        sx_top20_msg = build_table(stock_xgboost_score_top20, 'Stock XGBoost TOP20交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock XGBoost TOP20交易成绩清单", trading_date)

    try:
        sk_top5_msg = build_table(stock_keras_score_top5, 'Stock Keras TOP5交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock Keras TOP5交易成绩清单", trading_date)

    try:
        sk_top10_msg = build_table(stock_keras_score_top10, 'Stock Keras TOP10交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock Keras TOP10交易成绩清单", trading_date)

    try:
        sk_top20_msg = build_table(stock_keras_score_top20, 'Stock Keras TOP20交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock Keras TOP20交易成绩清单", trading_date)

    try:
        ix_top5_msg = build_table(index_xgboost_score_top5, 'Index XGBoost TOP5交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Index XGBoost TOP5交易成绩清单", trading_date)

    try:
        ik_top5_msg = build_table(index_keras_score_top5, 'Index Keras TOP5交易成绩清单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Index Keras TOP5交易成绩清单", trading_date)

    try:
        sx_top5_his = build_table(stock_xgboost_his_top5, 'Stock XGBoost TOP5交易详单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock XGBoost TOP5交易详单", trading_date)

    try:
        sk_top5_his = build_table(stock_keras_his_top5, 'Stock Keras TOP5交易详单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Stock Keras TOP5交易详单", trading_date)

    try:
        ix_top5_his = build_table(index_xgboost_his_top5, 'Index XGBoost TOP5交易详单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Index XGBoost TOP5交易详单", trading_date)

    try:
        ik_top5_his = build_table(index_keras_his_top5, 'Index Keras TOP5交易详单')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:Index Keras TOP5交易详单", trading_date)

    try:
        hold_body = build_table(positions, '目前持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前持仓", trading_date)

    try:
        fronzen_body = build_table(frozen_positions, '目前锁定持仓')
    except:
        send_email('交易报告:'+ trading_date, "消息组件运算失败:目前锁定持仓", trading_date)

    try:
        msg = build_email(build_head(),err_msg,
                          top5_summary_msg,top10_summary_msg,top20_summary_msg,
                          sx_top5_msg,sx_top10_msg,sx_top20_msg,
                          sk_top5_msg,sk_top10_msg,sk_top20_msg,
                          ix_top5_msg,ik_top5_msg,
                          sx_top5_his,sk_top5_his,
                          ix_top5_his,ik_top5_his,
                          hold_body,fronzen_body
                          )
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

    return(stock_xgboost_tar[stock_xgboost_tar.RANK <= 5])




