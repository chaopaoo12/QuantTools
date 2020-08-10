import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import (accuracy_score,classification_report,precision_score)
from QUANTTOOLS.FactorTools.QuantMk import get_index_quant_data
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_log_info,QA_util_get_trade_range
import joblib
from QUANTTOOLS.FactorTools.base_func import mkdir
from sklearn import feature_selection
import numpy as np

class model():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def get_data(self, start, end, type ='crawl'):
        QA_util_log_info('##JOB Got Data by {type} ==== from {_from} to {_to}'.format(type=type, _from=start, _to=end), ui_log = None)
        self.data = get_index_quant_data(start, end, type = type)
        print(self.data.shape)

    def set_target(self, col, mark, type = 'value'):
        self.target = col
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} in column {col} ==== {date}'.format(type = type, mark=mark,
                                                                                                         col =col,date = self.info['date']),
                         ui_log = None)
        if type == 'value':
            self.data['star'] = self.data[self.target].apply(lambda x : 1 if x > mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data[self.target].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            QA_util_log_info('##target type must be in [value,percent] ==-== {}'.format(self.info['date']), ui_log = None)

        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)
        self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                               'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3','cate',
                                                               'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                               'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                               'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
        self.info['cols'] = self.cols

    def set_train_rng(self, train_start, train_end):
        QA_util_log_info('##JOB Set Train Range from {_from} to {_to} ==-== {date}'.format(_from=train_start,_to=train_end, date=self.info['date']), ui_log = None)
        self.TR_RNG = QA_util_get_trade_range(train_start, train_end)
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self, percent=13):
        QA_util_log_info('##JOB Split Train Data ===== {}'.format(self.info['date']), ui_log = None)
        self.X_train, self.Y_train = self.data.loc[self.TR_RNG][self.cols].fillna(0),self.data.loc[self.TR_RNG]['star'].fillna(0)
        QA_util_log_info('##JOB Feature Selection ===== {}'.format(self.info['date']), ui_log = None)
        self.fs = feature_selection.SelectPercentile(feature_selection.chi2, percentile=percent)
        self.X_train = self.fs.fit_transform(self.X_train, self.Y_train)
        self.info['fs'] = self.fs

    def build_model(self, other_params):
        QA_util_log_info('##JOB Set Model Params ===== {}'.format(self.info['date']), ui_log = None)
        #self.model = XGBClassifier(n_estimators = n_estimators, max_depth = max_depth, subsample= subsample,seed=seed)
        other_params = other_params
        self.model = XGBClassifier(**other_params)

    def model_running(self):
        QA_util_log_info('##JOB Now Model Traning ===== {}'.format(self.info['date']), ui_log = None)
        self.model.fit(self.X_train,self.Y_train)

        QA_util_log_info('##JOB Now Model Scoring ===== {}'.format(self.info['date']), ui_log = None)
        y_pred = self.model.predict(self.X_train)

        accuracy_train = accuracy_score(self.Y_train,y_pred)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        self.info['train_report'] = self.train_report

    def model_check(self):

        QA_util_log_info('##JOB Now Model Checking ===== {}'.format(self.info['date']), ui_log = None)

        if self.info['train_report']['1']['precision'] <0.75:
            print("精确率不足,模型需要优化")
            self.info['train_status']['precision'] = False
        else:
            self.info['train_status']['precision'] = True

        if self.info['train_report']['1']['recall'] < 0.3:
            print("召回率不足,模型需要优化")
            self.info['train_status']['recall'] = False
        else:
            self.info['train_status']['recall'] = True

        if self.info['train_status']['precision'] == False or self.info['train_status']['recall'] == False:
            self.info['train_status']['status'] = False
        else:
            self.info['train_status']['status'] = True


    def save_model(self, name, working_dir = 'D:\\model\\current'):
        QA_util_log_info('##JOB Now Model Saving ===== {}'.format(self.info['date']), ui_log = None)

        if mkdir(working_dir):
            try:
                joblib.dump(self.model, working_dir+"\\{name}.joblib.dat".format(name=name))
                joblib.dump(self.info, working_dir+"\\{name}_info.joblib.dat".format(name=name))
                print("dump success")
                return(True)
            except:
                print("dump fail")
                return(False)

    def model_important(self):
        QA_util_log_info('##JOB Now Got Model Importance ===== {}'.format(self.info['date']), ui_log = None)
        importance = pd.DataFrame({'featur' :list(np.asarray(self.info['cols'])[np.asarray(self.info['fs'].get_support())]),
                                   'value':list(self.model.feature_importances_)}).sort_values(by='value',ascending=False)
        return(importance)

def load_model(name, working_dir= 'D:\\model\\current'):
    QA_util_log_info('##JOB Now Model Loading', ui_log = None)
    model = joblib.load(working_dir+"\\{name}.joblib.dat".format(name=name))
    QA_util_log_info('##JOB Now Model Info Loading', ui_log = None)
    info = joblib.load(working_dir+"\\{name}_info.joblib.dat".format(name=name))
    return(model, info)

def model_predict(model, start, end, cols, fs, type='crawl'):
    QA_util_log_info('##JOB Now Got Prediction Data ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    data = get_index_quant_data(start, end, type= type)

    QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    cols1 = [i for i in data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                  'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3','cate',
                                                  'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                  'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                  'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
    train = pd.DataFrame()
    n_cols = []
    for i in cols:
        if i in cols1:
            train[i] = data[i].astype('float')
        else:
            train[i] = 0
            n_cols.append(i)
    train.index = data.index
    QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    QA_util_log_info(n_cols)

    train = fs.transform(train[cols].fillna(0))
    QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    b = data[['INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
    b = b.assign(y_pred = model.predict(train))
    bina = pd.DataFrame(model.predict_proba(train))[[0,1]]
    bina.index = b.index
    b[['Z_PROB','O_PROB']] = bina
    b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
    return(b[b['y_pred']==1], b)

def check_model(model, start, end, cols, col, target, type = 'value'):
    tar,b = model_predict(model, start,end, cols)
    if type == 'value':
        b['star'] = b[col].apply(lambda x : 1 if x >= target else 0)
    elif type == 'percent':
        b['star'] = b[col].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= target else 0)
    else:
        print("target type must be in ['value','percent']")
    report = classification_report(b['star'],b['y_pred'], output_dict=True)
    c = b[b['RANK']<=5]
    top_report = classification_report(c['star'],c['y_pred'], output_dict=True)
    return(c, report, top_report)
