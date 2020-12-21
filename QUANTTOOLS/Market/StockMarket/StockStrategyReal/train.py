#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.StockModel.StrategyXgboostHour import QAStockXGBoostHour
from QUANTTOOLS.Model.StockModel.StrategyXgboost15Min import QAStockXGBoost15Min
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from QUANTTOOLS.Model.IndexModel.IndexXGboostHour import QAIndexXGBoostHour
from QUANTTOOLS.Model.IndexModel.IndexXGboost15Min import QAIndexXGBoost15Min
from .setting import working_dir, stock_day_set, stock_hour_set, stock_min_set, index_day_set, index_hour_set, stock_xg_set
from QUANTTOOLS.Market.MarketTools.train_tools import prepare_train, start_train, save_report, load_data, prepare_data
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day
from QUANTAXIS.QAUtil import QA_util_add_months
import datetime

def choose_model(date, working_dir=working_dir):
    stock_model = QAStockXGBoost()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl')

    stock_model = prepare_data(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = -5, col = 'TARGET5', type='percent')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, None, other_params, 0, 0.99)
    save_report(stock_model, 'stock_xg', working_dir)

def daymodel_train(date, working_dir=working_dir):
    stock_model = QAStockXGBoost()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', norm_type=None)

    stock_model = prepare_data(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, stock_xg_set, other_params, 0, 0.99)
    save_report(stock_model, 'stock_xg', working_dir)

    stock_model = prepare_data(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0, col = 'MACD', type='value', shift = -5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, stock_day_set, other_params, 0, 0.99)
    save_report(stock_model, 'stock_mars_day', working_dir)

def minmodel_train(date, working_dir=working_dir):
    min_model = QAStockXGBoost15Min()

    start_date = datetime.datetime.strftime(QA_util_add_months(date,-6), "%Y-%m") +'-01'
    end_date = date

    min_model = load_data(min_model, start_date, end_date, norm_type=None)

    min_model = prepare_data(min_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 1), mark = 0, col = 'TERNS_15M', type='value', shift = -15)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    min_model = start_train(min_model, stock_min_set, other_params, 0, 0.99)
    save_report(min_model, 'stock_mars_min', working_dir)

def hourmodel_train(date, working_dir=working_dir):
    hour_model = QAStockXGBoostHour()

    start_date = str(int(date[0:4])-1)+'-01-01'
    end_date = date

    hour_model = load_data(hour_model, start_date, end_date, norm_type=None)

    hour_model = prepare_data(hour_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 3), mark = 0, col = 'TERNS_HR', type='value', shift = -5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hour_model = start_train(hour_model, stock_hour_set, other_params, 0, 0.99)
    save_report(hour_model, 'stock_mars_hour', working_dir)

def train_hedge(date, working_dir=working_dir):
    hedge_model = QAStockXGBoost()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    hedge_model = load_data(hedge_model, start_date, end_date)

    hedge_model = prepare_data(hedge_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), col = 'TARGET')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hedge_model = start_train(hedge_model, stock_day_set, other_params, 0, 0.99)
    save_report(hedge_model, 'hedge_xg', working_dir)

def train_index(date, working_dir=working_dir):
    index_model = QAIndexXGBoost()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    index_model = load_data(index_model, start_date, end_date, norm_type=None)

    index_model = prepare_data(index_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0, col = 'TERNS', type='value', shift = -5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    index_model = start_train(index_model, index_day_set, other_params, 0, 0.99)
    save_report(index_model, 'index_mars_day', working_dir)

    hour_model = QAIndexXGBoostHour()

    hour_model = load_data(hour_model, start_date, end_date, norm_type=None)

    hour_model = prepare_data(hour_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 3), mark = 0, col = 'TERNS_HR', type='value', shift = -5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hour_model = start_train(hour_model, index_hour_set, other_params, 0, 0.99)
    save_report(hour_model, 'index_mars_hour', working_dir)