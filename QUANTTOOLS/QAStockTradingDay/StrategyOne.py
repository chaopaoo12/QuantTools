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

def train_test_split_date(x, test_size=0.3):
    split_row = len(x) - int(test_size * len(x))
    x_train = x.iloc[:split_row]
    x_test = x.iloc[split_row:]
    return x_train, x_test

def mkdir(path):
    # 引入模块
    import os

    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        try:
            os.makedirs(path)

            print(path + ' 创建成功')
            return True
        except:
            return False
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')
        return True

class model():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['test_status']=dict()
        self.info['rng_status']=dict()

    def get_data(self, start, end, type ='crawl', block=True):
        self.data = get_quant_data(start, end, type = type, block=block)
        print(self.data.shape)

    def set_target(self, mark, type = 'value'):
        if type == 'value':
            self.data['star'] = self.data['TARGET'].apply(lambda x : 1 if x >= mark else 0)
            self.data.loc[self.data['PASS_MARK'] >= 9.95,'star'] = 0
        elif type == 'percent':
            self.data['star'] = self.data['TARGET'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
            self.data.loc[self.data['PASS_MARK'] >= 9.95,'star'] = 0
        else:
            print("target type must be in ['value','percent']")
        self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                               'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                               'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                               'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                               'INDEX_TARGET10','date_stamp']]
        self.info['cols'] = self.cols

    def set_train_rng(self, train_start, train_end, test_start, test_end):
        self.TR_RNG = pd.Series(pd.date_range(train_start, train_end, freq='D')).apply(lambda x: str(x)[0:10])
        self.TE_RNG = pd.Series(pd.date_range(test_start, test_end, freq='D')).apply(lambda x: str(x)[0:10])
        self.info['train_rng'] = [train_start,train_end]
        self.info['test_rng'] = [test_start,test_end]

    def prepare_data(self, type = 'date', test_size = 0.2, random_state=0):
        if type == 'date':
            self.train_rng, self.test_rng = train_test_split_date(self.TR_RNG, test_size)
            self.X_train, self.Y_train = self.data.loc[self.train_rng][self.cols].fillna(0),self.data.loc[self.train_rng]['star'].fillna(0)
            self.X_test, self.Y_test = self.data.loc[self.test_rng][self.cols].fillna(0),self.data.loc[self.test_rng]['star'].fillna(0)
        elif type == 'random':
            self.X_train, self.X_test, self.Y_train, self.Y_test = train_test_split(self.data.loc[self.TR_RNG][self.cols],self.data.loc[self.TR_RNG]['star'], test_size=test_size, random_state=random_state)
        else:
            print('type must be in [date, random]')
        self.X_RNG, self.Y_RNG = self.data.loc[self.TE_RNG][self.cols],self.data.loc[self.TE_RNG]['star']

    def build_model(self, n_estimators=500):
        self.model = XGBClassifier(n_estimators=n_estimators,seed=1)

    def model_running(self):
        self.model.fit(self.X_train,self.Y_train, eval_metric=list(["auc",'error','map']),
                       eval_set=[(self.X_test,self.Y_test),(self.X_RNG,self.Y_RNG)], verbose=True)
        y_pred = self.model.predict(self.X_train)
        y_pred_test = self.model.predict(self.X_test)
        y_pred_rng = self.model.predict(self.X_RNG)

        accuracy_train = accuracy_score(self.Y_train,y_pred)
        accuracy_test = accuracy_score(self.Y_test,y_pred_test)
        accuracy_rng = accuracy_score(self.Y_RNG,y_pred_rng)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        print("accuracy_test:"+str(accuracy_test)+"; precision_score On test:"+str(precision_score(self.Y_test, y_pred_test)))
        self.test_report = classification_report(self.Y_test,y_pred_test, output_dict=True)
        print(self.test_report)
        print("accuracy_rng:"+str(accuracy_rng)+"; precision_score On rng:"+str(precision_score(self.Y_RNG,y_pred_rng)))
        self.rng_report = classification_report(self.Y_RNG,y_pred_rng, output_dict=True)
        print(self.rng_report)
        self.info['train_report'] = self.train_report
        self.info['test_report'] = self.test_report
        self.info['rng_report'] = self.rng_report

    def model_check(self):
        if self.info['test_report']['1']['precision'] <0.75:
            print("精确率不足,模型需要优化")
            self.info['train_status']['precision'] = False

        elif self.info['test_report']['1']['recall'] < 0.3:
            print("召回率不足,模型需要优化")
            self.info['train_status']['recall'] = False
        else:
            self.info['train_status']['precision'] = True
            self.info['train_status']['recall'] = True


        if abs(self.info['train_report']['1']['precision'] - self.info['test_report']['1']['precision']) > 0.1:
            print("过拟合:精确率差异过大")
            self.info['test_status']['precision'] = False
        elif abs(self.info['train_report']['1']['recall'] - self.info['test_report']['1']['recall']) > 0.05:
            print("过拟合:召回差异过大")
            self.info['test_status']['recall'] = False
        else:
            self.info['test_status']['precision'] = True
            self.info['test_status']['recall'] = True


        if abs(self.info['test_report']['1']['precision'] - self.info['rng_report']['1']['precision']) > 0.1:
            print("风险:测试集与校验集结果差异显著 精确率差异过大")
            self.info['rng_status']['precision'] = False
        elif abs(self.info['test_report']['1']['recall'] - self.info['rng_report']['1']['recall']) > 0.05:
            print("风险:测试集与校验集结果差异显著 召回差异过大")
            self.info['rng_status']['recall'] = False
        else:
            self.info['rng_status']['precision'] = True
            self.info['rng_status']['recall'] = True

        if self.info['train_status']['precision'] == False or self.info['train_status']['recall'] == False:
            self.info['train_status']['status'] = False
        else:
            self.info['train_status']['status'] = True

        if self.info['test_status']['precision'] == False or self.info['test_status']['recall'] == False:
            self.info['test_status']['status'] = False
        else:
            self.info['test_status']['status'] = True

        if self.info['rng_status']['precision'] == False or self.info['rng_status']['recall'] == False:
            self.info['rng_status']['status'] = False
        else:
            self.info['rng_status']['status'] = True

    def save_model(self, working_dir = 'D:\\model\\current'):
        if mkdir(working_dir):
            try:
                joblib.dump(self.model, working_dir+"\\current.joblib.dat")
                joblib.dump(self.info, working_dir+"\\current_info.joblib.dat")
                print("dump success")
                return(True)
            except:
                print("dump fail")
                return(False)

def load_model(working_dir= 'D:\\model\\current'):
    model = joblib.load(working_dir+"\\current.joblib.dat")
    info = joblib.load(working_dir+"\\current_info.joblib.dat")
    return(model, info)

def model_predict(model, start, end, cols):
    data = get_quant_data(start, end, type='crawl',block = True)
    cols1 = [i for i in data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO','LAG_TORO','OPEN_MARK','PASS_MARK',
                                                  'TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET',
                                                  'INDEX_TARGET','INDUSTRY',
                                                  'INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10','date_stamp']]
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
    b['y_pred'] = model.predict(train)
    bina = pd.DataFrame(model.predict_proba(train))[[0,1]]
    bina.index = b.index
    b[['Z_PROB','O_PROB']] = bina
    b['RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
    return(b[b['y_pred']==1])

def check_model(model, start, end, cols, target):
    data = get_quant_data(start, end, type='crawl',block = True)
    data['star'] = data['TARGET'].apply(lambda x :1 if x >= target else 0)
    cols1 = [i for i in data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO','LAG_TORO','OPEN_MARK','PASS_MARK',
                                                  'TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET',
                                                  'INDEX_TARGET','INDUSTRY',
                                                  'INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10','date_stamp']]
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
    b = data[['star','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
    b['y_pred'] = model.predict(train)
    bina = pd.DataFrame(model.predict_proba(train))[[0,1]]
    bina.index = b.index
    b[['Z_PROB','O_PROB']] = bina
    b['RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
    report = classification_report(b['star'],b['y_pred'], output_dict=True)
    c = b[b['RANK']<=5]
    top_report = classification_report(c['star'],c['y_pred'], output_dict=True)
    return(report,top_report)
