#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboostNeut import QAStockXGBoostNeut
from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import working_dir, stock_day_set, index_day_set, stock_xg_set, index_xg_set, stock_day_nn, stock_xg_nn
from QUANTTOOLS.Market.MarketTools.TrainTools import start_train, save_report, load_data, prepare_data, set_target
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day


def neut_model(date, working_dir=working_dir):
    stock_model = QAStockXGBoostNeut()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', norm_type=None)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model = prepare_data(stock_model, stock_xg_set, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg_neut', working_dir)

def choose_model(date, working_dir=working_dir):
    stock_model = QAStockXGBoost()

    start_date = str(int(date[0:4])-3)+'-01-01'
    end_date = date

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', norm_type=None)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model = prepare_data(stock_model, stock_xg_set, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg', working_dir)

def daymodel_train(date, working_dir=working_dir):
    stock_model = QAStockXGBoostNeut()

    start_date = str(int(date[0:4])-1)+'-01-01'
    end_date = date

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', sub_block=True, norm_type=None)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model = prepare_data(stock_model, stock_xg_set, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg_nn', working_dir)

    stock_model.data = stock_model.data[stock_model.data.OPEN_MARK == 0]
    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET', type='percent')

    stock_model = prepare_data(stock_model, stock_day_set, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_mars_nn', working_dir)

    stock_model = QAStockXGBoost()

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', sub_block=True, norm_type=None, ST=False)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model = prepare_data(stock_model, stock_xg_nn, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg', working_dir)

    stock_model.data = stock_model.data[stock_model.data.OPEN_MARK == 0]
    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET', type='percent')

    stock_model = prepare_data(stock_model, stock_day_nn, 0, 0.95)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_mars_day', working_dir)



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

    index_model = load_data(index_model, start_date, end_date, type='crawl', norm_type=None)

    index_model = prepare_data(index_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'INDEX_TARGET5', type='percent')

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    index_model = start_train(index_model, index_xg_set, other_params, 0, 0.95)
    save_report(index_model, 'index_xg', working_dir)

    index_model = prepare_data(index_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0, col = 'SKDJ_TR', type='value', shift = -2)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    index_model = start_train(index_model, index_day_set, other_params, 0, 0.95)
    save_report(index_model, 'index_mars_day', working_dir)