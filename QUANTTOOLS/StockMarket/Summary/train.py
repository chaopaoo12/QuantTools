#coding=utf-8

from QUANTTOOLS.StockMarket.StockStrategyThird.setting import working_dir
from QUANTTOOLS.QAStockTradingDay.StockModel.StrategyXgboost import model as StockModelXGBosst
from QUANTTOOLS.QAStockTradingDay.StockModel.StrategyKeras import model as StockModelKeras
from QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexKeras import model as IndexModelKeras
from QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexXGboost import model as IndexModelXGBosst
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
import pandas as pd
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day

def train(date, strategy_id='机器学习1号', working_dir=working_dir, ui_log = None):
    QA_util_log_info('##JOB01 Now Stock XGBoost Model Init ==== {}'.format(str(date)), ui_log)
    stock_xgbosst = StockModelXGBosst()

    QA_util_log_info('##JOB02 Now Stock XGBoost Prepare Model Data ==== {}'.format(str(date)), ui_log)
    stock_xgbosst.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date, block=False, sub_block=False)
    QA_util_log_info('##JOB03 Now Set Stock Model Target ==== {}'.format(str(date)), ui_log)
    stock_xgbosst.set_target(mark =0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Stock XGBoost Model Train time range ==== {}'.format(str(date)), ui_log)
    stock_xgbosst.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                              train_end=QA_util_get_last_day(QA_util_get_real_date(date), 5))
    stock_xgbosst.prepare_data()
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 0,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
    stock_xgbosst.build_model(other_params)
    QA_util_log_info(
        '##JOB05 Now Stock XGBoost Model Trainnig ==== {}'.format(str(date)), ui_log)
    stock_xgbosst.model_running()
    QA_util_log_info(
        '##JOB06 Now Save Stock XGBoost Model ==== {}'.format(str(date)), ui_log)
    important = stock_xgbosst.model_important()
    stock_xgbosst.save_model('stock',working_dir = working_dir)
    stock_train_report = build_table(pd.DataFrame(stock_xgbosst.info['train_report']), '个股模型训练集情况')
    stock_ft_importance = build_table(important.head(100), '个股模型特征重要性')

    del stock_xgbosst

    QA_util_log_info('##JOB06 Now Stock XGBoost Trainning Report ==== {}'.format(str(date)), ui_log)
    msg1 = 'Stock XGBoost模型训练日期:{model_date}'.format(model_date=stock_xgbosst.info['date'])
    msg = build_email(build_head(),msg1,
                      stock_train_report,
                      stock_ft_importance)
    send_email('模型训练报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      'Stock XGBoost模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    QA_util_log_info('##JOB01 Now Stock Keras Model Init ==== {}'.format(str(date)), ui_log)
    stock_keras = StockModelKeras()
    important.head(100)
    QA_util_log_info('##JOB02 Now Stock Keras Prepare Model Data ==== {}'.format(str(date)), ui_log)
    stock_keras.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date, block=False, sub_block=False)
    QA_util_log_info('##JOB03 Now Set Stock Keras Model Target ==== {}'.format(str(date)), ui_log)
    stock_keras.set_target(mark =0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Stock Keras Model Train time range ==== {}'.format(str(date)), ui_log)
    stock_keras.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                              train_end=QA_util_get_last_day(QA_util_get_real_date(date), 5))
    stock_keras.prepare_data(thres=0,cols = list(important.head(100).featur))
    stock_keras.build_model(loss = 'binary_crossentropy')
    QA_util_log_info('##JOB05 Now Stock Keras Model Trainnig ==== {}'.format(str(date)), ui_log)
    stock_keras.model_running(batch_size=4096, nb_epoch=100)
    QA_util_log_info('##JOB06 Now Save Stock Keras Model ==== {}'.format(str(date)), ui_log)
    stock_keras.save_model('stock',working_dir = working_dir)
    stock_train_report = build_table(pd.DataFrame(stock_keras.info['train_report']), '个股模型训练集情况')

    del stock_keras

    QA_util_log_info('##JOB06 Now Stock Keras Trainning Report ==== {}'.format(str(date)), ui_log)
    msg1 = 'Stock Keras模型训练日期:{model_date}'.format(model_date=stock_keras.info['date'])
    msg = build_email(build_head(),msg1,
                      stock_train_report)
    send_email('模型训练报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      'Stock Keras模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    QA_util_log_info('##JOB01 Now Index XGBoost Model Init ==== {}'.format(str(date)), ui_log)
    index_xgboost = IndexModelXGBosst()
    QA_util_log_info('##JOB02 Now Prepare Index XGBoost Model Data ==== {}'.format(str(date)), ui_log)
    index_xgboost.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date)
    QA_util_log_info('##JOB03 Now Set Index XGBoost Model Target ==== {}'.format(str(date)), ui_log)
    index_xgboost.set_target('INDEX_TARGET5', mark = 0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Index XGBoost Model Train time range ==== {}'.format(str(date)), ui_log)
    index_xgboost.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                                train_end=QA_util_get_last_day(QA_util_get_real_date(date), 5))
    index_xgboost.prepare_data()
    #other_params = {'learning_rate': 0.1, 'n_estimators': 100, 'max_depth': 3, 'min_child_weight': 3, 'seed': 0,
    #                'subsample': 0.75, 'colsample_bytree': 0.65, 'gamma': 0.1, 'reg_alpha': 0.05, 'reg_lambda': 1}
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 0,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
    index_xgboost.build_model(other_params)
    QA_util_log_info('##JOB05 Now Index XGBoost Model Trainnig ==== {}'.format(str(date)), ui_log)
    index_xgboost.model_running()
    index_important = index_xgboost.model_important()
    QA_util_log_info('##JOB06 Now Save Index XGBoost Model ==== {}'.format(str(date)), ui_log)
    index_xgboost.save_model('index',working_dir = working_dir)
    index_train_report = build_table(pd.DataFrame(index_xgboost.info['train_report']), '指数模型训练集情况')

    del index_xgboost

    msg1 = 'Index XGBoost模型训练日期:{model_date}'.format(model_date=index_xgboost.info['date'])
    msg = build_email(build_head(),msg1,
                      index_train_report)
    send_email('模型训练报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      'Index XGBoost模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    QA_util_log_info('##JOB01 Now Index Keras Model Init ==== {}'.format(str(date)), ui_log)
    index_keras = IndexModelKeras()

    QA_util_log_info('##JOB02 Now Prepare Index Keras Model Data ==== {}'.format(str(date)), ui_log)
    index_keras.get_data(start=str(int(date[0:4])-3)+"-01-01", end=date)
    QA_util_log_info('##JOB03 Now Set Index Keras Model Target ==== {}'.format(str(date)), ui_log)
    index_keras.set_target('INDEX_TARGET5', mark = 0.3, type = 'percent')
    QA_util_log_info('##JOB04 Now Set Index Keras Model Train time range ==== {}'.format(str(date)), ui_log)
    index_keras.set_train_rng(train_start=str(int(date[0:4])-3)+"-01-01",
                              train_end=QA_util_get_last_day(QA_util_get_real_date(date), 5))
    index_keras.prepare_data(thresh=0,cols = list(index_important.head(100).featur))
    index_keras.build_model(loss = 'binary_crossentropy')
    QA_util_log_info('##JOB05 Now Index Keras Model Trainnig ==== {}'.format(str(date)), ui_log)
    index_keras.model_running(batch_size=4096, nb_epoch=100)
    QA_util_log_info('##JOB06 Now Save Index Keras Model ==== {}'.format(str(date)), ui_log)
    index_keras.save_model('index',working_dir = working_dir)
    index_train_report = build_table(pd.DataFrame(index_keras.info['train_report']), '指数模型训练集情况')

    del index_keras

    QA_util_log_info('##JOB06 Now Index Keras Trainning Report ==== {}'.format(str(date)), ui_log)
    msg1 = 'Index Keras模型训练日期:{model_date}'.format(model_date=index_keras.info['date'])
    msg = build_email(build_head(),msg1,
                      index_train_report)
    send_email('模型训练报告', msg, 'date')
    send_actionnotice(strategy_id,
                      '报告:{}'.format(date),
                      'Index Keras模型训练完成,请查收结果',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )
