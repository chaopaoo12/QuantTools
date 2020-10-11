#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboostReal import QAStockXGBoostReal
from .setting import working_dir, data_set, datareal_set

from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from multiprocessing import Process
from QUANTAXIS.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day
from QUANTAXIS.QAUtil import (QA_util_log_info)
import pandas as pd

def prepare_train(date, ui_log = None):
    QA_util_log_info('##JOB01 Now Model Init ==== {}'.format(str(date)), ui_log)
    stock_model = QAStockXGBoostReal()

    QA_util_log_info('##JOB02 Now Stock Prepare Model Data ==== {}'.format(str(date)), ui_log)
    stock_model.get_data(start=str(int(date[0:4])-3)+"-01-01", end= QA_util_get_last_day(QA_util_get_real_date(date), 1), block=False, sub_block=False)
    QA_util_log_info('##JOB03 Now Set Stock Model Target ==== {}'.format(str(date)), ui_log)
    stock_model.set_target(mark =0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Stock Model Train time range ==== {}'.format(str(date)), ui_log)
    stock_model.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                              train_end=QA_util_get_last_day(QA_util_get_real_date(date), 1))
    return(stock_model)

def start_train(stock_model, cols, name, other_params, thresh=0, drop=0.99, working_dir = working_dir):

    stock_model.prepare_data(thresh=thresh, drop=drop, cols = cols)
    other_params = other_params
    stock_model.build_model(other_params)
    stock_model.model_running()
    stock_model.save_model(name,working_dir = working_dir)
    important = stock_model.model_important()
    stock_train_report = build_table(pd.DataFrame(stock_model.info['train_report']), '个股模型训练集情况')
    stock_ft_importance = build_table(important.head(50), '个股模型特征重要性')

    msg1 = '{name}模型训练日期:{model_date}'.format(name=name, model_date=stock_model.info['date'])

    QA_util_log_info('##JOB06 Now Model Trainning Report ==== {}'.format(str(stock_model.info['train_rng'][1])))
    msg = build_email(build_head(),msg1,
                      stock_train_report,stock_ft_importance)
    try:
        send_email('{name}模型训练报告'.format(name=name), msg, stock_model.info['train_rng'][1])
        send_actionnotice('{name}模型训练报告'.format(name=name),
                          '报告:{}'.format(stock_model.info['train_rng'][1]),
                          '{name}模型训练完成,请查收结果'.format(name=name),
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    except:
        pass
    return(0)

def train_mult(date):
    stock_model = prepare_train(date)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    p = Process(target=start_train, args=(stock_model, datareal_set, 'stock_xg_real', other_params, 0, 0.99))
    p.start()
    p = Process(target=start_train, args=(stock_model, data_set, 'stock_xg', other_params, 0, 0.99))
    p.start()
    p.join()