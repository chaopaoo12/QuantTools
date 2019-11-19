
from QUANTTOOLS.QAStockTradingDay.StrategyOne import model, load_model, model_predict, check_model
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_basic_info_tushare
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
import logging
import strategyease_sdk
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice


def predict(date, strategy_id='机器学习1号', account1='name:client-1', working_dir=working_dir, ui_log = None):
    try:
        QA_util_log_info(
            '##JOB01 Now Got Account Info ==== {}'.format(str(date)), ui_log)
        logging.basicConfig(level=logging.DEBUG)
        client = strategyease_sdk.Client(host=yun_ip, port=yun_port, key=easytrade_password)
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
    except:
        send_email('错误报告', '云服务器错误,请检查', 'date')
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    try:
        QA_util_log_info(
            '##JOB02 Now Load Model ==== {}'.format(str(date)), ui_log)
        model_temp,info_temp = load_model(working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', 'date')
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    QA_util_log_info(
        '##JOB03 Now Model Predict ==== {}'.format(str(date)), ui_log)
    tar = model_predict(model_temp, str(date[0:7])+"-01",date,info_temp['cols'])

    QA_util_log_info(
        '##JOB03 Now Concat Result ==== {}'.format(str(date)), ui_log)
    res = pd.concat([tar[tar['RANK'] <= 5].loc[date][['Z_PROB','O_PROB','RANK']],
                     QA_fetch_stock_day_adv(list(tar[tar['RANK'] <= 5].loc[date].index),date,date).data.loc[date].reset_index('date')['close'],
                     QA_fetch_stock_fianacial_adv(list(tar[tar['RANK'] <= 5].loc[date].index),date,date).data.reset_index('date')[['NAME','INDUSTRY']]],
                    axis=1)

    QA_util_log_info(
        '##JOB04 Now Funding Decision ==== {}'.format(str(date)), ui_log)
    avg_account = sub_accounts['总 资 产']/tar[tar['RANK'] <= 5].loc[date].shape[0]
    res = res.assign(tar=avg_account[0])
    res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
    res['real'] = res['cnt'] * res['close']

    QA_util_log_info(
        '##JOB05 Now Current Report ==== {}'.format(str(date)), ui_log)
    table1 = tar[tar['RANK']<=5].groupby('date').mean()

    QA_util_log_info(
        '##JOB06 Now Current Holding ==== {}'.format(str(date)), ui_log)
    positions = client.get_positions(account1)['positions'][['证券代码','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)','当前持仓']]

    QA_util_log_info(
        '##JOB07 Now Message Building ==== {}'.format(str(date)), ui_log)
    msg1 = '模型训练日期:{model_date}'.format(model_date=info_temp['date'])
    body1 = build_table(table1, '近段时间内模型盈利报告')
    body2 = build_table(res, '目标持仓')
    body3 = build_table(positions, '目前持仓')

    msg = build_email(build_head(),msg1,body1,body2,body3)

    send_email('交易报告:'+ date, msg, 'date')





