from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne import load_model, model_predict
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.account_manage import trade_roboot
from QUANTTOOLS.QAIndexTradingDay.IndexStrategyFirst.setting import working_dir, percent
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.account_manage import get_Client
import time
import datetime


def trading(trading_date,percent=percent, strategy_id= '机器学习1号', account1= 'name:client-1', working_dir= working_dir, ui_log= None):

    try:
        QA_util_log_info(
            '##JOB01 Now Load Model ==== {}'.format(str(trading_date)), ui_log)
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
        '##JOB02 Now Model Predict ==== {}'.format(str(trading_date)), ui_log)
    tar,tar1 = model_predict(model_temp, str(trading_date[0:7])+"-01",trading_date,info_temp['cols'])
    r_tar = tar[tar['RANK'] <= 5].loc[trading_date][['Z_PROB','O_PROB','RANK']]
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


