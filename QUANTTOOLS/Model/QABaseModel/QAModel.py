
from QUANTAXIS.QAUtil import (QA_util_log_info, QA_util_today_str)
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_trade_range
import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
from sklearn.utils import shuffle
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import normalization, standardize, series_to_supervised
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old,QA_fetch_stock_all,QA_fetch_code_new,QA_fetch_stock_om_all
import numpy as np
import pandas as pd
import re

class QAModel():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def set_data(self, data):
        self.data = data
        QA_util_log_info(self.data.shape)

    def set_target(self, col, mark, type = 'value', shift= None):
        self.target = col
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} in column {col} ==== {date}'.format(
            type=type, mark= mark,col=col, date= self.info['date']),ui_log = None)

        if type == 'value':
            self.data['star'] = self.data[self.target].apply(lambda x: 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data[self.target].groupby('date').apply(
                lambda x: x.rank(ascending=False, pct=True)).apply(
                lambda x :1 if x <= mark else 0)
        elif type == 'reg':
            self.data['star'] = self.data[self.target]
        else:
            QA_util_log_info('##target type must be in [value,percent] ===== {}'.format(self.info['date']), ui_log = None)

        if shift is not None:
            self.data['star'] = self.data['star'].groupby('code').shift(shift)

        self.info['target'] = self.target
        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)


    def set_train_rng(self, train_start, train_end):
        QA_util_log_info('##JOB Set Train Range from {_from} to {_to} ===== {date}'.format(_from=train_start,_to=train_end, date=self.info['date']), ui_log = None)
        self.TR_RNG = QA_util_get_trade_range(train_start, train_end)
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self, thresh = None, drop = 0, cols = None, n_in= None,train_type=True):

        self.thresh = thresh
        self.drop = drop
        self.n_in = n_in

        if cols is None:
            self.cols = [i for i in self.data.columns if i not in
                         ['moon','star','mars','venus','sun','MARK','date','datetime',
                          'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                          'DATE_STAMP_15M', 'DATE_STAMP_30M','TIME_STAMP_15M','TIME_STAMP_30M',
                          'TARGET4','TARGET5','TARGET10','TARGET20','AVG_TARGET','INDEX_TARGET',
                          'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                          'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]
        else:
            self.cols = cols
        QA_util_log_info('cols')
        QA_util_log_info(cols)
        QA_util_log_info('self.cols')
        QA_util_log_info(self.cols)

        self.data = self.data.fillna(value=np.nan)

        self.shuffle()
        non_cols, std_cols = self.desribute_check()
        QA_util_log_info('##JOB Drop Columns with low {} fill rate {} ===== {}'.format(drop, non_cols, self.info['date']), ui_log = None)
        self.cols = [i for i in self.cols if i not in std_cols + non_cols]

        loss_rate = self.thresh_check(train_type=train_type)
        QA_util_log_info('##JOB Split Train Data ===== {}'.format(self.info['date']), ui_log = None)

        if 'date' in list(self.data.columns):
            self.X_train, self.Y_train = shuffle(self.train_data[self.train_data.date.isin(self.TR_RNG)][self.cols],
                                                 self.train_data[self.train_data.date.isin(self.TR_RNG)]['star'])
        else:
            self.X_train, self.Y_train = shuffle(self.train_data.loc[self.TR_RNG][self.cols],
                                                 self.train_data.loc[self.TR_RNG]['star'])

        self.info['thresh'] = thresh
        self.info['drop'] = drop
        self.info['cols'] = self.cols

    def normoalize_data(self, type='normal'):
        if type == 'normal':
            self.X_train = self.X_train.groupby('date').apply(normalization)
        elif type == 'stand':
            self.X_train = self.X_train.groupby('date').apply(standardize)
        self.info['normoalize'] = type

    def copy_model(self, object):
        self.data = object.data
        self.info = object.info

    def save_model(self, name, working_dir = 'D:\\model\\current'):
        QA_util_log_info('##JOB Now Model Saving ===== {}'.format(self.info['date']), ui_log = None)

        if mkdir(working_dir):
            try:
                joblib.dump(self.model, working_dir+"\\{name}.joblib.dat".format(name=name))
                joblib.dump(self.info, working_dir+"\\{name}info.joblib.dat".format(name=name))
                QA_util_log_info("dump success")
                return(True)
            except:
                QA_util_log_info("dump fail")
                return(False)

    def load_model(self, name, working_dir= 'D:\\model\\current'):
        QA_util_log_info('##JOB Now Model Loading', ui_log = None)
        self.model = joblib.load(working_dir+"\\{name}.joblib.dat".format(name=name))
        self.info = joblib.load(working_dir+"\\{name}info.joblib.dat".format(name=name))

        try:
            self.norm_type = self.info['norm_type']
        except:
            self.norm_type = None
            self.info['norm_type'] = None

        try:
            self.block = self.info['block']
        except:
            self.info['block'] = False
            self.block = False

        try:
            self.sub_block = self.info['sub_block']
        except:
            self.info['sub_block'] = False
            self.sub_block = False

        try:
            self.normoalize = self.info['normoalize']
        except:
            self.info['normoalize'] = None
            self.normoalize = None

        try:
            self.n_in = self.info['n_in']
        except:
            self.n_in = None
            self.info['n_in'] = None

        try:
            self.cols = self.info['cols']
        except:
            self.info['cols'] = None
            self.cols = None

        return(self)

    def get_param(self):
        for k, v in self.info.items():
            setattr(self, k, v)

        if hasattr(self, 'n_in'):
            pass
        else:
            self.n_in = None

        if hasattr(self, 'normoalize'):
            pass
        else:
            self.normoalize = None

    def base_predict(self):
        self.data = self.data.assign(y_pred = self.model.predict(self.data[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(self.data[self.cols]))[[0,1]]
        bina.index = self.data.index
        self.data[['Z_PROB','O_PROB']] = bina
        self.data = self.data[self.data['O_PROB'].notna()]

    def desribute_check(self):
        s_res = self.data[self.cols].describe().T
        s_res = s_res.assign(rate = s_res['count']/self.data.shape[0])
        non_cols = list(s_res[s_res.rate < self.drop].index)
        std_cols = list(s_res[s_res['std']==0].index)
        return(non_cols, std_cols)

    def thresh_check(self, train_type=False):
        nan_num = (self.data[self.cols].isnull().sum(axis=1)> 0).sum()
        loss_rate = nan_num/self.data.shape[0]
        QA_util_log_info('##JOB Clean Data With {per} ({NAN_NUM}/{shape})  Contain NAN ==== '.format(
            NAN_NUM=nan_num, per = loss_rate, shape=self.data.shape[0]), ui_log = None)
        b_num = self.data.shape[0]

        if 'date' in list(self.data.columns):
            add_cols = ['star','date']
        else:
            add_cols = ['star']

        if train_type is True:
            QA_util_log_info('##JOB Thresh Clean Data With Train Type ==== ', ui_log = None)

            if self.thresh is None:
                self.train_data = self.data[self.cols + add_cols]
            elif self.thresh == 0:
                self.train_data = self.data[self.cols + add_cols].dropna().join(
                    self.data[[i for i in list(self.data.columns) if i not in self.cols + ['star']]])

                QA_util_log_info('##JOB Delete Data With {per} ({NAN_NUM}/{shape})  Contain NAN ==== '.format(
                    NAN_NUM=b_num - self.train_data.shape[0], per = 1-self.train_data.shape[0]/b_num, shape=b_num), ui_log = None)
            else:
                self.train_data = self.data[self.cols + add_cols].dropna(thresh=(len(self.cols) - self.thresh)).join(
                    self.data[[i for i in list(self.data.columns) if i not in self.cols + ['star']]])
                QA_util_log_info('##JOB Delete Data With {per} ({NAN_NUM}/{shape})  Contain NAN ==== '.format(
                    NAN_NUM=b_num - self.train_data.shape[0], per = 1-self.train_data.shape[0]/b_num, shape=b_num), ui_log = None)
        else:
            QA_util_log_info('##JOB Thresh Clean Data No Train Type ==== ', ui_log = None)
            if self.thresh is None:
                pass
            elif self.thresh == 0:
                self.data = self.data[self.cols + ['star']].dropna().join(
                    self.data[[i for i in list(self.data.columns) if i not in self.cols + ['star']]])
                QA_util_log_info('##JOB Delete Data With {per} ({NAN_NUM}/{shape})  Contain NAN ==== '.format(
                    NAN_NUM=b_num - self.data.shape[0], per = 1-self.data.shape[0]/b_num, shape=b_num), ui_log = None)
            else:
                self.data = self.data.dropna(subset=self.cols,thresh=(len(self.cols) - self.thresh))
                QA_util_log_info('##JOB Delete Data With {per} ({NAN_NUM}/{shape})  Contain NAN ==== '.format(
                    NAN_NUM=b_num - self.data.shape[0], per = 1-self.data.shape[0]/b_num, shape=b_num), ui_log = None)

        send_email('模型训练报告', "数据损失比例 {}".format(loss_rate), self.info['date'])
        if loss_rate >= 0.01:
            send_actionnotice('模型运行报告',
                              '数据检查报告:',
                              "数据损失比例过高 {}".format(loss_rate),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )
        return(loss_rate)

    def data_reshape(self):
        cols1 = [i for i in self.data.columns if i not in [ 'moon','star','mars','venus','sun','MARK','date','datetime',
                                                       'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                       'TARGET4','TARGET5','TARGET10','TARGET20','AVG_TARGET','INDEX_TARGET',
                                                       'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                       'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]
        n_cols = []
        for i in self.cols:
            if i in cols1:
                pass
            else:
                self.data[i] = 0
                n_cols.append(i)
        return(n_cols)

    def code_check(self):
        code_all = QA_fetch_stock_all()['code'].unique().tolist()
        code_old = QA_fetch_code_old()['code'].unique().tolist()
        code_new = QA_fetch_code_new()['code'].unique().tolist()
        codes = QA_fetch_stock_om_all()
        if self.code is None:
            codes = codes
        else:
            codes = codes[codes.code.isin(self.code)]

        ST = list(codes[codes.name.apply(lambda x:x.count('ST')) == 1]['code']) + list(codes[codes.name.apply(lambda x:x.count('退')) == 1]['code'])
        codes = list(codes['code'])
        code_688 = [i for i in codes if i.startswith('688') == True] + [i for i in codes if i.startswith('787') == True] + [i for i in codes if i.startswith('789') == True]

        short_of_code = [i for i in code_all if i not in code_old + code_new]
        if len(short_of_code) > 0:
            QA_util_log_info('##JOB {} Short of Code: {} ===== {}'.format(
                len(short_of_code), short_of_code, self.trading_date), ui_log = None)

            send_actionnotice('股票列表数据缺失',
                              '缺失警告',
                              "缺少股票".format(len(short_of_code)),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

        target_code = [i for i in code_all if i not in code_new + ST + code_688]
        short_of_data = [i for i in target_code if i not in
                         self.data.loc[self.trading_date].reset_index().code.unique().tolist()]

        if len(short_of_data) > 0:
            QA_util_log_info('##JOB {} Short of Data: {} ===== {_from}'.format(
                len(short_of_data), short_of_data, _from=self.trading_date), ui_log = None)
            send_actionnotice('基础数据缺失',
                              '缺失警告:{}'.format(self.trading_date),
                              "缺少数量".format(len(short_of_data)),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

        return(short_of_code, short_of_data)

    def shuffle(self, cols = None, n_in = None):

        QA_util_log_info('##JOB01 Now Data shuffle {}'.format(self.n_in))

        cols = list(set([re.sub(r"\((.*?)\)|\{(.*?)\}|\[(.*?)\]", "", i) for i in self.cols]))

        if self.n_in is not None:
            if cols is not None:
                shuffle_data = self.data[cols].groupby('code').apply(series_to_supervised, n_in = self.n_in)
            else:
                shuffle_data = self.data[
                    [i for i in self.data.columns if i not in
                     ['next_date','OPEN_MARK','PRE_DATE','PASS_MARK',
                      'TARGET','TARGET3','TARGET4','TARGET5','TARGET10','TARGET20']]].groupby('code').apply(
                    series_to_supervised, n_in = self.n_in)

            self.data = shuffle_data.join(self.data)

        self.info['n_in'] = self.n_in
        QA_util_log_info('##JOB01 Now Data shuffle Finish')
        QA_util_log_info(self.data.shape)


if __name__ == 'main':
    pass
