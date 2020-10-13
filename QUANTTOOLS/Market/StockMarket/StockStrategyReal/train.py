#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboostReal import QAStockXGBoostReal
from QUANTTOOLS.Model.StockModel.StrategyXgboostHedge import QAStockModelHedgeReal
from .setting import working_dir, data_set, datareal_set
from QUANTTOOLS.Market.MarketTools.train_tools import prepare_train, start_train

def train(date, working_dir=working_dir):
    stock_model = QAStockXGBoostReal()
    stock_model = prepare_train(stock_model, date)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    start_train(stock_model, datareal_set, 'stock_xg_real', other_params, 0, 0.99, working_dir)

    start_train(stock_model, data_set, 'stock_xg', other_params, 0, 0.99, working_dir)


def train_hedge(date, working_dir=working_dir):
    stock_model = QAStockModelHedgeReal()
    stock_model = prepare_train(stock_model, date)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    start_train(stock_model, datareal_set, 'hedge_xg', other_params, 0, 0.99, working_dir)

