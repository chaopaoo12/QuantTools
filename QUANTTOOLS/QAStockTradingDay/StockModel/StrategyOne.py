import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import (accuracy_score,confusion_matrix,
                             classification_report,roc_curve,roc_auc_score,
                             auc,precision_score,recall_score,f1_score)

from sklearn.model_selection import train_test_split
from QUANTTOOLS.FactorTools.base_func import get_quant_data
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter, QA_util_log_info, QA_util_add_months,
                              QA_util_to_json_from_pandas, QA_util_today_str,QA_util_get_pre_trade_date,
                              QA_util_datetime_to_strdate)
import joblib
from QUANTTOOLS.FactorTools.base_func import mkdir

class model():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def get_data(self, start, end, type ='crawl', block=True, sub_block=True):
        self.data = get_quant_data(start, end, type = type, block=block, sub_block= sub_block)
        self.data = self.data[self.data['DAYSO']>= 90][self.data['next_date'] == self.data['PRE_DATE']]
        print(self.data.shape)

    def set_target(self, mark, type = 'value'):
        if type == 'value':
            self.data['star'] = self.data['TARGET5'].apply(lambda x : 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data['TARGET5'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            print("target type must be in ['value','percent']")
        self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                               'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                               'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                               'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                               'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
        self.info['cols'] = self.cols

    def set_train_rng(self, train_start, train_end):
        self.TR_RNG = pd.Series(pd.date_range(train_start, train_end, freq='D')).apply(lambda x: str(x)[0:10])
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self):

        self.X_train, self.Y_train = self.data.loc[self.TR_RNG][self.cols].fillna(0),self.data.loc[self.TR_RNG]['star'].fillna(0)

    def build_model(self, other_params):
        #self.model = XGBClassifier(n_estimators = n_estimators, max_depth = max_depth, subsample= subsample,seed=seed)
        other_params = other_params
        self.model = XGBClassifier(**other_params)

    def model_running(self):
        self.model.fit(self.X_train,self.Y_train)
        y_pred = self.model.predict(self.X_train)

        accuracy_train = accuracy_score(self.Y_train,y_pred)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        self.info['train_report'] = self.train_report

    def model_check(self):

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
        importance = pd.DataFrame({'featur' :list(self.X_train.columns),'value':list(self.model.feature_importances_)}).sort_values(by='value',ascending=False)
        return(importance)

def load_model(name, working_dir= 'D:\\model\\current'):
    model = joblib.load(working_dir+"\\{name}.joblib.dat".format(name=name))
    info = joblib.load(working_dir+"\\{name}_info.joblib.dat".format(name=name))
    return(model, info)

def model_predict(model, start, end, cols, type='crawl', block = True, sub_block= True):
    data = get_quant_data(start, end, type= type,block = block, sub_block=sub_block)
    cols1 = [i for i in data.columns if i not in [ 'moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                   'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3',
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
    print(n_cols)
    b = data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
    b = b.assign(y_pred = model.predict(train[cols].fillna(0)))
    bina = pd.DataFrame(model.predict_proba(train[cols].fillna(0)))[[0,1]]
    bina.index = b.index
    b[['Z_PROB','O_PROB']] = bina
    b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
    return(b[b['y_pred']==1], b)

def check_model(model, start, end, cols, target, type = 'value',block=True, sub_block=True):
    tar,b = model_predict(model, start,end, cols, block = block, sub_block= sub_block)
    if type == 'value':
        b['star'] = b['TARGET5'].apply(lambda x : 1 if x >= target else 0)
    elif type == 'percent':
        b['star'] = b['TARGET5'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= target else 0)
    else:
        print("target type must be in ['value','percent']")
    report = classification_report(b['star'],b['y_pred'], output_dict=True)
    c = b[b['RANK']<=5]
    top_report = classification_report(c['star'],c['y_pred'], output_dict=True)
    return(c, report, top_report)
