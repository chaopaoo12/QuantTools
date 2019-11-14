
from QUANTTOOLS.QAStockTradingDay.StrategyOne import model, load_model, model_predict, check_model
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_basic_info_tushare
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
import logging
import strategyease_sdk
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from datetime import datetime,timedelta
delta = timedelta(days=6)
delta1 = timedelta(days=1)
delta3 = timedelta(days=7)
delta4 = timedelta(days=8)


def predict(date, account1='name:client-1', working_dir=working_dir):
    try:
        logging.basicConfig(level=logging.DEBUG)
        client = strategyease_sdk.Client(host=yun_ip, port=yun_port, key=easytrade_password)
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
    except:
        send_email('错误报告', '云服务器错误,请检查', 'date')

    try:
        model_temp,info_temp = load_model(working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', 'date')

    tar = model_predict(model_temp, str(date[0:7])+"-01",date,info_temp['cols'])
    res = pd.concat([tar[tar['RANK'] <= 5].loc[date][['Z_PROB','O_PROB','RANK']],
                     QA_fetch_stock_day_adv(list(tar[tar['RANK'] <= 5].loc[date].index),date,date).data.loc[date].reset_index('date')['close'],
                     QA_fetch_stock_fianacial_adv(list(tar[tar['RANK'] <= 5].loc[date].index),date,date).data.reset_index('date')[['NAME','INDUSTRY']]],
                    axis=1)
    #计算资金分配
    avg_account = sub_accounts['可用金额']/tar[tar['RANK'] <= 5].loc[date].shape[0]
    res = res.assign(tar=avg_account[0])
    res['cnt'] = (res['tar']/res['close']/100).apply(lambda x:round(x,0)*100)
    res['real'] = res['cnt'] * res['close']

    ##近日盈利情况
    table1 = tar[tar['RANK']<=5].groupby('date').mean()

    ##当前持仓
    positions = client.get_positions(account1)['positions'][['证券代码','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)','当前持仓']]

    msg1 = '模型训练日期:{model_date}'.format(model.info['date'])
    body1 = build_table(table1, '近段时间内模型盈利报告')
    body2 = build_table(res, '目标持仓')
    body3 = build_table(positions, '目前持仓')

    msg = build_email(build_head(),msg1,body1,body2,body3)

    send_email('交易报告', msg, 'date')





