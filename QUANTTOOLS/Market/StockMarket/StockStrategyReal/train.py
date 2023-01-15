#coding=utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboostNeut import QAStockXGBoostNeut
from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import working_dir, exceptions, trading_setting, \
    stock_day_set, index_day_set, stock_xg_set, index_xg_set, stock_day_nn, stock_xg_nn, block_set, data_set, in_set, out_set
from QUANTTOOLS.Market.MarketTools.TrainTools import start_train, save_report, load_data, prepare_data, set_target, shuffle
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_real_date,QA_util_get_last_day,QA_util_get_pre_trade_date
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.running import watch_func1, watch_func
from QUANTTOOLS.Model.StockModel.StrategyXgboostMin import QAStockXGBoostMin
import QUANTTOOLS.Market.MarketTools.DataTools as DataTools
from QUANTTOOLS.Trader import get_Client,check_Client
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.concat_predict import concat_predict, concat_predict_neut


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

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', sub_block=True, norm_type=None, ST=True)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model = prepare_data(stock_model, stock_xg_nn, 0, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg_nn', working_dir)

    stock_model.data = stock_model.data[stock_model.data.OPEN_MARK == 0]
    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET', type='percent')


    stock_model = prepare_data(stock_model, stock_day_nn, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_mars_nn', working_dir)

    stock_model = QAStockXGBoost()

    stock_model = load_data(stock_model, start_date, end_date, type ='crawl', sub_block=True, norm_type=None, ST=True)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET5', type='percent')

    stock_model.data = stock_model.data[stock_model.data.OPEN_MARK == 0]

    stock_model = prepare_data(stock_model, stock_xg_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_xg', working_dir)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 0.3, col = 'TARGET', type='percent')

    stock_model = prepare_data(stock_model, stock_day_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_mars_day', working_dir)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 5, col = 'TARGET5', type='value')
    stock_model = prepare_data(stock_model, data_set[0:50], None, 0.95, [1,2,3,5,10,15,20], train_type=True)

    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_sh', working_dir)

    res_a, res_b, res_c, res_d = watch_func(start_date, end_date)

    stock_model.data = stock_model.data.reindex(res_b.index)
    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 6), mark = 5, col = 'TARGET', type='value')

    stock_model = prepare_data(stock_model, None, 0, 0, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)

    stock_model = prepare_data(stock_model, stock_model.info['importance'].head(100).featur.tolist(), None, 0.01)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'block_day', working_dir)



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


def train_min_model(date, working_dir=working_dir):
    ui_log = None
    strategy_id=''
    trading_date=date
    account= 'name:client-1'
    working_dir = working_dir

    client = get_Client(type='yun_ease',trader_path=None,host=trading_setting['host'],port=trading_setting['port'],key=trading_setting['key'])
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
    try:
        positions = positions.code.tolist()
    except:
        positions = []
    r_tar, xg_sh, prediction = DataTools.load_data(concat_predict, trading_date, working_dir, 'stock_sh', 'prediction_sh')
    xg_sh=xg_sh[xg_sh.RANK<=20]

    r_tar, xg, prediction = DataTools.load_data(concat_predict, trading_date, working_dir, 'stock_xg', 'prediction')
    r_tar, xg_nn, prediction = DataTools.load_data(concat_predict_neut, trading_date, working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
    r_tar, mars_nn, prediction = DataTools.load_data(concat_predict_neut, trading_date, working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
    r_tar, mars_day, prediction = DataTools.load_data(concat_predict, trading_date, working_dir, 'stock_mars_day', 'prediction_stock_mars_day')
    code_list = list(set(xg_sh[(xg_sh.RANK <= 20)&(xg_sh.TARGET5.isnull())].reset_index().code.tolist()
                         + xg[(xg.RANK <= 20)&(xg.TARGET5.isnull())].reset_index().code.tolist()
                         + xg_nn[(xg_nn.RANK <= 20)&(xg_nn.TARGET5.isnull())].reset_index().code.tolist()
                         + mars_nn[(mars_nn.RANK <= 20)&(mars_nn.TARGET5.isnull())].reset_index().code.tolist()
                         + mars_day[(mars_day.RANK <= 20)&(mars_day.TARGET5.isnull())].reset_index().code.tolist()
                         + positions))

    start_date = QA_util_get_last_day(QA_util_get_real_date(date), 30)
    end_date = date

    stock_model = QAStockXGBoostMin()

    stock_model = load_data(stock_model, QA_util_get_last_day(start_date, 15), end_date, type ='crawl', sub_block=True,
                            norm_type=None, ST=True,code=code_list)

    stock_model = set_target(stock_model, start_date, QA_util_get_last_day(QA_util_get_real_date(date), 3), mark = 0.03,
                             col = 'TARGET', type='value')

    stock_model = prepare_data(stock_model, in_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_in', working_dir)

    stock_model.data = stock_model.data.assign(star=stock_model.data.TARGET3.apply(lambda x: 1 if x >= 0.03 else 0))
    stock_model = prepare_data(stock_model, in_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_in_1', working_dir)

    stock_model.data = stock_model.data.assign(star=stock_model.data.TARGET.apply(lambda x: 1 if x <= -0.02 else 0))
    stock_model = prepare_data(stock_model, in_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_out', working_dir)

    stock_model.data = stock_model.data.assign(star=stock_model.data.TARGET3.apply(lambda x: 1 if x <= -0.03 else 0))
    stock_model = prepare_data(stock_model, in_set, None, 0.95, train_type=True)
    other_params = {'learning_rate': 0.1, 'n_estimators': 200, 'max_depth': 5, 'min_child_weight': 1, 'seed': 1,
                    'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

    stock_model = start_train(stock_model, other_params)
    save_report(stock_model, 'stock_out_1', working_dir)