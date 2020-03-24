
import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyOne as Stock
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne as Index
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
from QUANTTOOLS.FactorTools.base_tools import combine_model
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.StockStrategyFirst.setting import working_dir, percent
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

    QA_util_log_info(
        '##JOB03 Now Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #index_list,index_report,index_top_report = Index.check_model(index_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),index_info_temp['cols'], 'INDEXT_TARGET5', 0.3)
    index_tar,index_b  = Index.model_predict(index_model_temp, str(trading_date[0:7])+"-01",trading_date,index_info_temp['cols'])

    #safe_list,safe_report,safe_top_report = Index.check_model(safe_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),safe_info_temp['cols'], 'INDEXT_TARGET', 0.3)
    safe_tar,safe_b  = Index.model_predict(safe_model_temp, str(trading_date[0:7])+"-01",trading_date,index_info_temp['cols'])

    stock_list,report,top_report = Stock.check_model(stock_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),stock_info_temp['cols'], 0.42)
    stock_tar,stock_b  = Stock.model_predict(stock_model_temp, str(trading_date[0:7])+"-01",trading_date,stock_info_temp['cols'])

    tar = combine_model(index_b, stock_b, safe_b, str(trading_date[0:7])+"-01",trading_date)
    QA_util_log_info(
        '##JOB03 Now Concat Result ==== {}'.format(str(trading_date)), ui_log)
    print(tar)
    try:
        tar1 = tar.loc[trading_date]
    except:
        tar1 = None
    if tar1 is None:
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')
    else:
        tar2 = tar1[['Z_PROB','O_PROB','RANK']]
        print(trading_date)
        close = QA_fetch_stock_day_adv(list(tar1.index),trading_date,trading_date).data.loc[trading_date].reset_index('date')['close']
        info = QA_fetch_stock_fianacial_adv(list(tar1.index),trading_date,trading_date).data.reset_index('date')[['NAME','INDUSTRY']]
        res = tar2.join(close).join(info)
        #res = pd.concat([tar2,close,info],axis=1)

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
            msg1 = '模型训练日期:{model_date}'.format(model_date=stock_info_temp['date'])
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
    return(tar)




