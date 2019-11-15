#coding=utf-8

from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTTOOLS.QAStockTradingDay.StrategyOne import model
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
import pandas as pd
from datetime import datetime,timedelta
delta = timedelta(days=6)
delta1 = timedelta(days=1)
delta3 = timedelta(days=7)
delta4 = timedelta(days=8)

def train(date, working_dir=working_dir):
    model1 = model()
    model1.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date)
    model1.set_target(mark =0.42, type = 'percent')
    model1.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                        train_end=(datetime.strptime(date, "%Y-%m-%d")-delta4).strftime('%Y-%m-%d'),
                        test_start=(datetime.strptime(date, "%Y-%m-%d")-delta3).strftime('%Y-%m-%d'),
                        test_end=date)
    model1.prepare_data()
    model1.build_model()
    model1.model_running()
    model1.model_check()
    model1.save_model(working_dir = working_dir)

    msg1 = '模型训练日期:{model_date}'.format(model.info['date'])
    body1 = build_table(pd.DataFrame(model.info['train_report']), '训练集情况')
    body2 = build_table(pd.DataFrame(model.info['test_report']), '测试集情况')
    #body3 = build_table(positions, '目前持仓')

    msg = build_email(build_head(),msg1,body1,body2)

    send_email('交易报告', msg, 'date')