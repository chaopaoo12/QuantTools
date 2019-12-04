#coding=utf-8

from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTTOOLS.QAStockTradingDay.StrategyOne import model
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
import pandas as pd
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice
from datetime import datetime,timedelta
delta = timedelta(days=6)
delta1 = timedelta(days=1)
delta3 = timedelta(days=7)
delta4 = timedelta(days=8)

def train(date, strategy_id='机器学习1号', working_dir=working_dir, ui_log = None):
    QA_util_log_info(
        '##JOB01 Now Model Init ==== {}'.format(str(date)), ui_log)
    model1 = model()
    QA_util_log_info(
        '##JOB02 Now Prepare Date ==== {}'.format(str(date)), ui_log)
    model1.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date)
    QA_util_log_info(
        '##JOB03 Now Set Target ==== {}'.format(str(date)), ui_log)
    model1.set_target(mark =0.42, type = 'percent')
    QA_util_log_info(
        '##JOB04 Now Set Train time range ==== {}'.format(str(date)), ui_log)
    model1.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                        train_end=(datetime.strptime(date, "%Y-%m-%d")-delta4).strftime('%Y-%m-%d'),
                        test_start=(datetime.strptime(date, "%Y-%m-%d")-delta3).strftime('%Y-%m-%d'),
                        test_end=date)
    model1.prepare_data()
    model1.build_model(n_estimators=1000)
    QA_util_log_info(
        '##JOB05 Now Model Trainnig ==== {}'.format(str(date)), ui_log)
    model1.model_running()
    model1.model_check()
    QA_util_log_info(
        '##JOB06 Now Save Model ==== {}'.format(str(date)), ui_log)
    important = model1.model_important()
    model1.save_model(working_dir = working_dir)
    QA_util_log_info(
        '##JOB06 Now Model Trainning Report ==== {}'.format(str(date)), ui_log)
    msg1 = '模型训练日期:{model_date}'.format(model_date=model1.info['date'])
    body1 = build_table(pd.DataFrame(model1.info['train_report']), '训练集情况')
    body2 = build_table(pd.DataFrame(model1.info['test_report']), '测试集情况')
    body3 = build_table(important.head(50), '特征重要性')

    msg = build_email(build_head(),msg1,body1,body2,body3)

    send_email('交易报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      '模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )