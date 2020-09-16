#coding=utf-8

from .setting import working_dir
from QUANTTOOLS.Model.StockModel.StrategyKeras import QAStockKeras
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
import pandas as pd
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTAXIS.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day

def train(date, strategy_id='机器学习1号', working_dir=working_dir, ui_log = None):
    QA_util_log_info('##JOB01 Now Model Init ==== {}'.format(str(date)), ui_log)
    stock_model = QAStockKeras()

    QA_util_log_info('##JOB02 Now Stock Prepare Model Data ==== {}'.format(str(date)), ui_log)
    stock_model.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date, block=False, sub_block=False)
    QA_util_log_info('##JOB03 Now Set Stock Model Target ==== {}'.format(str(date)), ui_log)
    stock_model.set_target(mark =0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Stock Model Train time range ==== {}'.format(str(date)), ui_log)
    stock_model.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                              train_end=QA_util_get_last_day(QA_util_get_real_date(date), 5))
    stock_model.prepare_data()
    stock_model.build_model(loss = 'binary_crossentropy')
    QA_util_log_info('##JOB05 Now Stock Model Trainnig ==== {}'.format(str(date)), ui_log)
    stock_model.model_running(batch_size=4096, nb_epoch=100)
    QA_util_log_info('##JOB06 Now Save Stock Model ==== {}'.format(str(date)), ui_log)
    important = stock_model.model_important()
    stock_model.save_model('stock',working_dir = working_dir)
    stock_train_report = build_table(pd.DataFrame(stock_model.info['train_report']), '个股模型训练集情况')
    stock_ft_importance = build_table(important.head(50), '个股模型特征重要性')

    msg1 = '模型训练日期:{model_date}'.format(model_date=stock_model.info['date'])
    del stock_model

    QA_util_log_info('##JOB06 Now Model Trainning Report ==== {}'.format(str(date)), ui_log)
    msg = build_email(build_head(),msg1,
                      stock_train_report,stock_ft_importance)

    send_email('模型训练报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      '模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )