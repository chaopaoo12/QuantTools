
from QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne import load_model, model_predict, check_model
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAIndexTradingDay.IndexStrategyFirst.setting import working_dir, percent
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTAXIS.QAUtil import QA_util_get_last_day
from QUANTTOOLS.account_manage import get_Client

def predict(trading_date, strategy_id='机器学习1号', account1='name:client-1', working_dir=working_dir, ui_log = None):

    try:
        QA_util_log_info(
            '##JOB01 Now Got Account Info ==== {}'.format(str(trading_date)), ui_log)
        client = get_Client()
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
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
        model_temp,info_temp = load_model('index',working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    QA_util_log_info(
        '##JOB03 Now Model Predict ==== {}'.format(str(trading_date)), ui_log)
    stock_list,report,top_report = check_model(model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),info_temp['cols'], 0.42)
    tar,b  = model_predict(model_temp, str(trading_date[0:7])+"-01",trading_date,info_temp['cols'])

    QA_util_log_info(
        '##JOB03 Now Concat Result ==== {}'.format(str(trading_date)), ui_log)
    tar1 = tar[tar['RANK'] <= 5].loc[trading_date]
    tar2 = tar1[['Z_PROB','O_PROB','RANK']]
    print(trading_date)
    close = QA_fetch_stock_day_adv(list(tar1.index),trading_date,trading_date).data.loc[trading_date].reset_index('date')['close']
    info = QA_fetch_stock_fianacial_adv(list(tar1.index),trading_date,trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
    res = pd.concat([tar2,close,info],axis=1)

    QA_util_log_info(
        '##JOB04 Now Funding Decision ==== {}'.format(str(trading_date)), ui_log)
    avg_account = sub_accounts['总 资 产']/tar1.shape[0]
    res = res.assign(tar=avg_account[0]*percent)
    res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
    res['real'] = res['cnt'] * res['close']

    QA_util_log_info(
        '##JOB05 Now Current Report ==== {}'.format(str(trading_date)), ui_log)
    table1 = tar[tar['RANK']<=5].groupby('date').mean()

    QA_util_log_info(
        '##JOB06 Now Current Holding ==== {}'.format(str(trading_date)), ui_log)
    positions = client.get_positions(account1)['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)']]

    QA_util_log_info(
        '##JOB07 Now Message Building ==== {}'.format(str(trading_date)), ui_log)
    try:
        msg1 = '模型训练日期:{model_date}'.format(model_date=info_temp['date'])
        body1 = build_table(table1, '近段时间内模型盈利报告')
        body2 = build_table(res, '目标持仓')
        body3 = build_table(positions, '目前持仓')
        body4 = build_table(pd.DataFrame(report), '上一交易日模型报告{}'.format(str(QA_util_get_last_day(trading_date))))
        body5 = build_table(pd.DataFrame(top_report), '上一交易日模型报告Top{}'.format(str(QA_util_get_last_day(trading_date))))
        body6 = build_table(stock_list, '上一交易日模型交易清单{}'.format(str(QA_util_get_last_day(trading_date))))

        msg = build_email(build_head(),msg1,body5,body4,body6,body1,body2,body3)
    except:
        send_email('交易报告:'+ trading_date, "消息构建失败", 'date')
    send_email('交易报告:'+ trading_date, msg, 'date')





