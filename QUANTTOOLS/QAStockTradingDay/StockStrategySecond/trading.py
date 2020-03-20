from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.FactorTools.base_tools import combine_model
import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyOne as Stock
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne as Index
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.account_manage import trade_roboot
from QUANTTOOLS.QAStockTradingDay.StockStrategyFirst.setting import working_dir, percent
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.account_manage import get_Client
import time
import datetime
from QUANTAXIS.QAUtil import QA_util_get_last_day

def trading(trading_date, percent=percent, strategy_id= '机器学习1号', account1= 'name:client-1', working_dir= working_dir, ui_log= None):

    try:
        QA_util_log_info(
            '##JOB01 Now Load Model ==== {}'.format(str(trading_date)), ui_log)
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
        '##JOB02 Now Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #tar,tar1 = model_predict(model_temp, str(trading_date[0:7])+"-01",trading_date,info_temp['cols'])

    #index_list,index_report,index_top_report = Index.check_model(index_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),index_info_temp['cols'], 'INDEXT_TARGET5', 0.3)
    index_tar,index_b  = Index.model_predict(index_model_temp, str(trading_date[0:7])+"-01",trading_date,index_info_temp['cols'])

    #safe_list,safe_report,safe_top_report = Index.check_model(safe_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),safe_info_temp['cols'], 'INDEXT_TARGET', 0.3)
    safe_tar,safe_b  = Index.model_predict(safe_model_temp, str(trading_date[0:7])+"-01",trading_date,index_info_temp['cols'])

    #stock_list,report,top_report = Stock.check_model(stock_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),stock_info_temp['cols'], 0.42)
    stock_tar,stock_b  = Stock.model_predict(stock_model_temp, str(trading_date[0:7])+"-01",trading_date,stock_info_temp['cols'])

    tar = combine_model(index_b, stock_b, safe_b, str(trading_date[0:7])+"-01",trading_date)
    try:
        r_tar = tar.loc[trading_date][['Z_PROB','O_PROB','RANK']]
    except:
        r_tar = None

    if r_tar is None:
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')
    else:
        try:
            QA_util_log_info(
                '##JOB03 Now Chect Account Server ==== {}'.format(str(trading_date)), ui_log)
            client = get_Client()
            account1=account1
            client.cancel_all(account1)
            account_info = client.get_account(account1)
            print(account_info)
        except:
            send_email('错误报告', '云服务器错误,请检查', trading_date)
            send_actionnotice(strategy_id,
                              '错误报告:{}'.format(trading_date),
                              '云服务器错误,请检查',
                              direction = 'HOLD',
                              offset='HOLD',
                              volume=None
                              )

        h1 = int(datetime.datetime.now().strftime("%H"))
        m1 = int(datetime.datetime.now().strftime("%M"))
        while h1 == 14 and m1 <= 50 :
            h1 = int(datetime.datetime.now().strftime("%H"))
            m1 = int(datetime.datetime.now().strftime("%M"))
            time.sleep(30)

        QA_util_log_info(
            '##JOB04 Now Trading ==== {}'.format(str(trading_date)), ui_log)
        res = trade_roboot(r_tar, account1, trading_date, percent, strategy_id)
        return(res)


