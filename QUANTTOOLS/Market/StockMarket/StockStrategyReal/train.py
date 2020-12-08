#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.StockModel.StrategyXgboostHour import QAStockXGBoostHour
from QUANTTOOLS.Model.StockModel.StrategyXgboost15Min import QAStockXGBoost15Min
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from QUANTTOOLS.Model.IndexModel.IndexXGboostHour import QAIndexXGBoostHour
from QUANTTOOLS.Model.IndexModel.IndexXGboost15Min import QAIndexXGBoost15Min
from .setting import working_dir, stock_day_set, stock_hour_set, stock_min_set, index_day_set, index_hour_set
from QUANTTOOLS.Market.MarketTools.train_tools import prepare_train, start_train, save_report, load_data, prepare_data

def train(date, working_dir=working_dir):
    stock_model = QAStockXGBoost()

    stock_model = load_data(stock_model, date, k = 3, start = "-01-01", norm_type=None)

    stock_model = prepare_data(stock_model, date, mark = -5, col = 'TERNS', type='shift', k = 3, start = "-01-01", shift=5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, stock_day_set, other_params, 0, 0.99)
    save_report(stock_model, 'stock_mars_day', working_dir)

    min_model = QAStockXGBoost15Min()
    min_model = load_data(min_model, date, k = 1, start = "-01-01", norm_type=None)

    min_model = prepare_data(min_model, date, mark = -5, col = 'TERNS_15M', type='shift', k = 3, start = "-01-01", shift=2)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    min_model = start_train(min_model, stock_min_set, other_params, 0, 0.99)
    save_report(min_model, 'stock_mars_min', working_dir)

    hour_model = QAStockXGBoostHour()
    hour_model = load_data(hour_model, date, k = 1, start = "-01-01", norm_type=None)

    hour_model = prepare_data(hour_model, date, mark = -5, col = 'TERNS_HR', type='shift', k = 3, start = "-01-01", shift=2)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hour_model = start_train(hour_model, stock_hour_set, other_params, 0, 0.99)
    save_report(hour_model, 'stock_mars_hour', working_dir)

def train_hedge(date, working_dir=working_dir):
    hedge_model = QAStockXGBoost()
    hedge_model = load_data(hedge_model, date, k = 3, start = "-01-01")

    hedge_model = prepare_data(hedge_model, date, col = 'TARGET', k = 3, start = "-01-01", shift=1)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hedge_model = start_train(hedge_model, stock_day_set, other_params, 0, 0.99)
    save_report(hedge_model, 'hedge_xg', working_dir)

def train_index(date, working_dir=working_dir):
    index_model = QAIndexXGBoost()
    index_model = load_data(index_model, date, k = 3, start = "-05-01", norm_type=None)

    index_model = prepare_data(index_model, date, mark = -5, col = 'TERNS', type='shift', k = 3, start = "-05-01", shift=5)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    index_model = start_train(index_model, index_day_set, other_params, 0, 0.99)
    save_report(index_model, 'index_mars_day', working_dir)

    hour_model = QAIndexXGBoostHour()
    hour_model = load_data(hour_model, date, k = 3, start = "-05-01", norm_type=None)

    hour_model = prepare_data(hour_model, date, mark = -5, col = 'TERNS_HR', type='shift', k = 3, start = "-05-01", shift=2)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    hour_model = start_train(hour_model, index_hour_set, other_params, 0, 0.99)
    save_report(hour_model, 'index_mars_hour', working_dir)

    min_model = QAIndexXGBoost15Min()
    min_model = load_data(min_model, date, k = 3, start = "-05-01", norm_type=None)

    min_model = prepare_data(min_model, date, mark = -5, col = 'TERNS_15M', type='shift', k = 3, start = "-05-01", shift=2)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    min_model = start_train(min_model, index_hour_set, other_params, 0, 0.99)
    save_report(min_model, 'index_mars_min', working_dir)