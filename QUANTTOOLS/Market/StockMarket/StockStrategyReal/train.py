#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboostReal import QAStockXGBoostReal
from QUANTTOOLS.Model.StockModel.StrategyXgboostHedge import QAStockModelHedgeReal
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from .setting import working_dir, data_set, datareal_set
from QUANTTOOLS.Market.MarketTools.train_tools import prepare_train, start_train, save_report

def train(date, working_dir=working_dir):
    stock_model = QAStockXGBoostReal()
    stock_model = prepare_train(stock_model, date, col = 'TARGET5',k = 0, start = "-09-01")

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, datareal_set,  other_params, 0, 0.99)
    save_report(stock_model, 'stock_xg_real', working_dir)

    stock_model = start_train(stock_model, data_set, other_params, 0, 0.99)
    save_report(stock_model, 'stock_xg', working_dir)

def train_hedge(date, working_dir=working_dir):
    stock_model = QAStockModelHedgeReal()
    stock_model = prepare_train(stock_model, date, col = 'TARGET')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, datareal_set, other_params, 0, 0.99)
    save_report(stock_model, 'hedge_xg', working_dir)

def train_index(date, working_dir=working_dir):
    index_model = QAIndexXGBoost()
    index_model = prepare_train(index_model, date, col = 'INDEX_TARGET')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    index_model = start_train(index_model, datareal_set, other_params, 0, 0.99)
    save_report(index_model, 'index_xg', working_dir)