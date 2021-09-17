
from QUANTAXIS.QAUtil import (QA_util_log_info, QA_util_today_str)
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import QA_util_get_trade_range
import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
from sklearn.utils import shuffle
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import normalization, standardize, series_to_supervised
import numpy as np

class QAModel():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def set_target(self, col, mark, type = 'value', shift= None):
        self.target = col
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} in column {col} ==== {date}'.format(type=type, mark= mark,
                                                                                                         col=col, date= self.info['date']),
                         ui_log = None)

        if type == 'value':
            if shift is not None:
                self.data['star'] = self.data[self.target].apply(lambda x: 1 if x >= mark else 0)
            else:
                self.data['star'] = self.data[self.target].apply(lambda x: 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data[self.target].groupby('date').apply(lambda x: x.rank(ascending=False, pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            QA_util_log_info('##target type must be in [value,percent] ===== {}'.format(self.info['date']), ui_log = None)

        if shift is not None:
            self.data['star'] = self.data['star'].groupby('code').shift(shift)

        self.info['target'] = self.target
        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)

    def shuffle(self, n_in = None):
        QA_util_log_info('##JOB01 Now Data shuffle {}'.format(n_in))
        if n_in is not None:
            self.data = self.data[[i for i in self.data.columns if i not in ['next_date','PRE_DATE','PASS_MARK','TARGET',
                                                                             'TARGET3','TARGET4','TARGET5','TARGET10','TARGET20']]].groupby('code').apply(series_to_supervised, n_in = n_in).join(self.data[['next_date','PRE_DATE','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','TARGET20']])
        self.info['n_in'] = n_in
        QA_util_log_info('##JOB01 Now Data shuffle Finish')


    def set_train_rng(self, train_start, train_end):
        QA_util_log_info('##JOB Set Train Range from {_from} to {_to} ===== {date}'.format(_from=train_start,_to=train_end, date=self.info['date']), ui_log = None)
        self.TR_RNG = QA_util_get_trade_range(train_start, train_end)
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self, thresh = None, drop = 0, cols= None):

        if cols is None:
            self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','date','datetime',
                                                                   'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                                   'TARGET4','TARGET5','TARGET10','TARGET20','AVG_TARGET','INDEX_TARGET',
                                                                   'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                                   'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]
        else:
            self.cols = cols

        self.data = self.data.fillna(value=np.nan)

        s_res = self.data[self.cols].describe().T
        s_res = s_res.assign(rate = s_res['count']/self.data.shape[0])
        std_cols = list(s_res[s_res['std']==0].index)
        QA_util_log_info('##JOB Drop Columns with 0 std {} ===== {}'.format(std_cols, self.info['date']), ui_log = None)
        #self.data = self.data.drop(columns=std_cols)
        self.cols = [i for i in self.cols if i not in std_cols]

        if drop > 0:
            non_cols = list(s_res[s_res.rate < drop].index)
            QA_util_log_info('##JOB Drop Columns with low {} fill rate {} ===== {}'.format(drop, non_cols, self.info['date']), ui_log = None)
            self.cols = [i for i in self.cols if i not in non_cols]

        if thresh is None:
            train_data = self.data
        else:
            nan_num = (self.data[self.cols].isnull().sum(axis=1)> 0).sum()
            QA_util_log_info('##JOB Drop Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ===== {date}'.format(
                NAN_NUM = nan_num, per=nan_num/self.data.shape[0], shape=self.data.shape[0], thresh=thresh,date=self.info['date']), ui_log = None)

            if nan_num/self.data.shape[0] >= 0.01:
                send_actionnotice('模型训练报告',
                                  '交易报告:{}'.format(self.info['date']),
                                  "数据损失比例过高 {}".format(nan_num/self.data.shape[0]),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )
            send_email('模型训练报告:'+ self.info['date'], "数据损失比例 {}".format(nan_num/self.data.shape[0]), self.info['date'])

            if thresh == 0:
                train_data = self.data[self.cols + ['star']].dropna().join(self.data[[i for i in list(self.data.columns) if i not in self.cols + ['star']]])
            else:
                train_data = self.data[self.cols + ['star']].dropna(thresh=(len(self.cols) - thresh)).join(self.data[[i for i in list(self.data.columns) if i not in self.cols + ['star']]])

        QA_util_log_info('##JOB Split Train Data ===== {}'.format(self.info['date']), ui_log = None)

        if 'date' in list(self.data.columns):
            self.X_train, self.Y_train = shuffle(train_data[train_data.date.isin(self.TR_RNG)][self.cols],train_data[train_data.date.isin(self.TR_RNG)]['star'])
        else:
            self.X_train, self.Y_train = shuffle(train_data.loc[self.TR_RNG][self.cols],train_data.loc[self.TR_RNG]['star'])

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
                print("dump success")
                return(True)
            except:
                print("dump fail")
                return(False)

    def load_model(self, name, working_dir= 'D:\\model\\current'):
        QA_util_log_info('##JOB Now Model Loading', ui_log = None)
        self.model = joblib.load(working_dir+"\\{name}.joblib.dat".format(name=name))
        self.info = joblib.load(working_dir+"\\{name}info.joblib.dat".format(name=name))
        self.cols = self.info['cols']
        self.thresh = self.info['thresh']
        self.drop = self.info['drop']
        self.code = self.info['code']
        self.target = self.info['target']

        try:
            self.norm_type = self.info['norm_type']
        except:
            pass

        try:
            self.block = self.info['block']
            self.sub_block = self.info['sub_block']
        except:
            pass

        try:
            self.normoalize = self.info['normoalize']
        except:
            pass

        try:
            self.n_in = self.info['n_in']
        except:
            self.n_in = None
        return(self)

if __name__ == 'main':
    pass
