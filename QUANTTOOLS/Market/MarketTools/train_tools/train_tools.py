#coding=utf-8

from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day
from QUANTAXIS.QAUtil import (QA_util_log_info)
import pandas as pd

def load_data(model, start_date, end_date, type ='model', norm_type=None, n_in=None, ui_log = None):
    QA_util_log_info('##JOB01 Now Model Init ==== {}'.format(str(end_date)), ui_log)

    QA_util_log_info('##JOB02 Now Stock Prepare Model Data ==== {}'.format(str(end_date)), ui_log)
    model.get_data(start=start_date, end= end_date, type =type, norm_type=norm_type, n_in = n_in)
    return(model)

def shuffle(model, n_in = None):
    model.shuffl(n_in)
    return(model)

def set_target(model, start_date, end_date, mark = 0.3, col = 'TARGET5', type = 'percent', shift = None, ui_log = None ):
    QA_util_log_info('##JOB03 Now Set Stock Model Target ==== {}'.format(str(end_date)), ui_log)
    model.set_target(col = col, mark = mark, type = type, shift = shift)
    QA_util_log_info('##JOB04 Now Set Stock Model Train time range ==== {}'.format(str(end_date)), ui_log)
    model.set_train_rng(train_start=start_date,
                        train_end=end_date)
    return(model)

def prepare_data(model, cols, thresh=0, drop=0.99):
    model.prepare_data(thresh=thresh, drop=drop, cols = cols)
    return(model)

def norm_data(model, type = 'normal', ui_log = None ):
    QA_util_log_info('##JOB03 Now Norm Stock Model Data ==== ', ui_log)
    model.normoalize_data(type=type)

    return(model)

def start_train(model, other_params):
    other_params = other_params
    model.build_model(other_params)
    model.model_running()
    return(model)

def save_report(model, name, working_dir=None):
    model.save_model(name,working_dir = working_dir)
    important = model.model_important()
    stock_train_report = build_table(pd.DataFrame(model.info['train_report']), '个股模型训练集情况')
    stock_ft_importance = build_table(important.head(50), '个股模型特征重要性')

    msg1 = '{name}模型训练日期:{model_date}'.format(name=name, model_date=model.info['date'])

    QA_util_log_info('##JOB06 Now Model Trainning Report ==== {}'.format(str(model.info['train_rng'][1])))
    msg = build_email(build_head(),msg1,
                      stock_train_report,stock_ft_importance)
    try:
        send_email('{name}模型训练报告'.format(name=name), msg, model.info['train_rng'][1])
        send_actionnotice('{name}模型训练报告'.format(name=name),
                          '报告:{}'.format(model.info['train_rng'][1]),
                          '{name}模型训练完成,请查收结果'.format(name=name),
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    except:
        pass
    return(0)


def prepare_train(model, date, col = 'TARGET5', k = 3, start = "-01-01", shift = 5, ui_log = None):
    QA_util_log_info('##JOB01 Now Model Init ==== {}'.format(str(date)), ui_log)

    QA_util_log_info('##JOB02 Now Stock Prepare Model Data ==== {}'.format(str(date)), ui_log)
    model.get_data(start=str(int(date[0:4])-k)+start, end= QA_util_get_last_day(QA_util_get_real_date(date), shift))
    QA_util_log_info('##JOB03 Now Set Stock Model Target ==== {}'.format(str(date)), ui_log)
    model.set_target(col = col, mark = 0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Stock Model Train time range ==== {}'.format(str(date)), ui_log)
    model.set_train_rng(train_start=str(int(date[0:4])-k)+start,
                        train_end=QA_util_get_last_day(QA_util_get_real_date(date), shift))
    return(model)

if __name__ == '__main__':
    pass