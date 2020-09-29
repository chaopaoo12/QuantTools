import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_norm
from QUANTAXIS.QAUtil import (QA_util_log_info, QA_util_today_str,QA_util_get_trade_range)
import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
from sklearn.utils import shuffle
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice

class QAStockModel():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def get_data(self, start, end, block=False, sub_block=False, type ='crawl'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data_norm(start, end, type = type, block = block, sub_block = sub_block)
        self.data = self.data[(self.data.next_date == self.data.PRE_DATE)]
        print(self.data.shape)

    def set_target(self, mark, type = 'value'):
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} ===== {date}'.format(type = type, mark=mark,
                                                                                          date = self.info['date']),
                         ui_log = None)
        self.data['moon'] = self.data['TARGET5'].apply(lambda x : 1 if x > 0 else 0)
        self.data['sun'] = self.data['TARGET'].apply(lambda x : 1 if x > 0 else 0)

        if type == 'value':
            self.data['star'] = self.data['TARGET'].apply(lambda x : 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data['TARGET'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            QA_util_log_info('##target type must be in [value,percent] ===== {}'.format(self.info['date']), ui_log = None)

        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)

    def set_train_rng(self, train_start, train_end):
        QA_util_log_info('##JOB Set Train Range from {_from} to {_to} ===== {date}'.format(_from=train_start,_to=train_end, date=self.info['date']), ui_log = None)
        self.TR_RNG = QA_util_get_trade_range(train_start, train_end)
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self,thresh = None, drop = 0, cols= None):

        if cols is None:
            self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK',
                                                                   'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                                   'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                                   'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                                   'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
        else:
            self.cols = cols

        s_res = self.data[self.cols].describe().T
        s_res = s_res.assign(rate = s_res['count']/self.data.shape[0])
        std_cols = list(s_res[s_res['std']==0].index)
        QA_util_log_info('##JOB Drop Columns with 0 std {} ===== {}'.format(std_cols, self.info['date']), ui_log = None)
        self.data = self.data.drop(columns=std_cols)
        self.cols = [i for i in self.cols if i not in std_cols]

        if drop > 0:
            non_cols = list(s_res[s_res.rate < drop].index)
            QA_util_log_info('##JOB Drop Columns with low {} fill rate {} ===== {}'.format(drop, non_cols, self.info['date']), ui_log = None)
            self.cols = [i for i in self.cols if i not in non_cols]

        if thresh is None:
            pass
        else:
            nan_num = self.data[self.cols].isnull().sum(axis=1)[self.data[self.cols].isnull().sum(axis=1) > 0].count()
            QA_util_log_info('##JOB Drop Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ===== {date}'.format(
                NAN_NUM = nan_num, per=nan_num/self.data.shape[0], shape=self.data.shape[0], thresh=thresh,date=self.info['date']), ui_log = None)
            if thresh == 0:
                self.data = self.data[self.cols].dropna().join(self.data[[i for i in list(self.data.columns) if i not in self.cols]])
            else:
                self.data = self.data[self.cols].dropna(thresh=(len(self.cols) - thresh)).join(self.data[[i for i in list(self.data.columns) if i not in self.cols]])

            send_email('模型训练报告:'+ self.info['date'], "数据损失比例 {}".format(nan_num/self.data.shape[0]), self.info['date'])
            if nan_num/self.data.shape[0] >= 0.01:
                send_actionnotice('模型训练报告',
                              '交易报告:{}'.format(self.info['date']),
                              "数据损失比例过高 {}".format(nan_num/self.data.shape[0]),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

        QA_util_log_info('##JOB Split Train Data ===== {}'.format(self.info['date']), ui_log = None)
        self.X_train, self.Y_train = shuffle(self.data.loc[self.TR_RNG][self.cols].fillna(0),self.data.loc[self.TR_RNG]['star'])
        self.info['thresh'] = thresh
        self.info['drop'] = drop

    def build_model(self):
        pass

    def model_running(self):
        pass

    def model_check(self):
        pass

    def model_important(self):
        pass

    def save_model(self, name, working_dir = 'D:\\model\\current'):
        self.info['cols'] = self.cols
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
        return(self)

    def model_predict(self, start, end, block = False, sub_block= False, type='crawl'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        data = get_quant_data_norm(start, end, type= type,block = block, sub_block=sub_block)

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        cols1 = [i for i in data.columns if i not in [ 'moon','star','mars','venus','sun','MARK',
                                                       'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                       'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                       'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                       'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
        train = pd.DataFrame()
        n_cols = []
        for i in self.cols:
            if i in cols1:
                train[i] = data[i].astype('float')
            else:
                train[i] = 0
                n_cols.append(i)
        train.index = data.index
        QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        QA_util_log_info(n_cols)

        if self.thresh is None:
            train = train[self.cols]
        else:
            nan_num = train[self.cols].isnull().sum(axis=1)[train[self.cols].isnull().sum(axis=1) > 0].count()
            QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain NAN ==== from {_from} to {_to}'.format(
                NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], _from=start,_to = end), ui_log = None)
            if self.thresh == 0:
                train = train[self.cols].dropna()
            else:
                train = train[self.cols].dropna(thresh=(len(self.cols) - self.thresh))

            send_email('模型训练报告:{}'.format(end) + end, "数据损失比例 {}".format(nan_num/train.shape[0]), self.info['date'])
            if nan_num/train.shape[0] >= 0.01:
                send_actionnotice('模型训练报告',
                                  '交易报告:{}'.format(end),
                                  "数据损失比例过高 {}".format(nan_num/train.shape[0]),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )

        train = train.join(data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict(train[self.cols]))
        b['O_PROB'] = self.model.predict_proba(train[self.cols])
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

if __name__ == 'main':
    pass
