from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_close
from QUANTTOOLS.StockMarket.StockStrategySecond.concat_predict import concat_predict,save_prediction
import pandas as pd
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.StockMarket.StockStrategySecond.setting import working_dir, percent, exceptions
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
    tar,index_tar,safe_tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    QA_util_log_info('##JOB03 Now Saving Result ==== {}'.format(str(trading_date)), ui_log)
    save_prediction({'date': trading_date, 'tar':tar}, 'prediction', working_dir)

    QA_util_log_info('##JOB04 Now Funding Decision ==== {}'.format(str(trading_date)), ui_log)
    try:
        tar1 = tar.loc[trading_date]
    except:
        tar1 = None

    if tar1 is None:
        res = None
    else:
        tar2 = tar1[['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']].reset_index()

        tar2 = tar2.assign(close= tar2['code'].apply(lambda x:QA_fetch_get_stock_close(str(x))))
        res = tar2.set_index('code')

        avg_account = (sub_accounts - frozen)/tar1.shape[0]
        res = res.assign(tar=avg_account*percent)
        res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
        res['real'] = res['cnt'] * res['close']

    QA_util_log_info('##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)
    #table1 = tar[tar['RANK']<=5].groupby('date').mean()
    if tar is not None and tar.shape[0] > 0:
        table1 = tar.groupby('date').mean()
    else:
        table1 = pd.DataFrame()
        tar = pd.DataFrame()

    QA_util_log_info('##JOB06 Now Message Building ==== {}'.format(str(trading_date)), ui_log)
    try:
        safe_res = safe_tar.loc[trading_date]
    except:
        safe_res = pd.DataFrame()

    try:
        index_res = index_tar.loc[trading_date]
    except:
        index_res = pd.DataFrame()

    try:
        stock_res = stock_tar[stock_tar['RANK']<=5].loc[trading_date]
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




