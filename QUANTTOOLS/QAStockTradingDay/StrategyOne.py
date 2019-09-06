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
import keras
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers import LSTM
from keras import backend as K
from keras.metrics import top_k_categorical_accuracy
import tensorflow as tf
#from tensorflow.keras.metrics import top_k_categorical_accuracy
import numpy as np

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

def recall(y_true, y_pred):
    # Calculates the recall
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
    recall = true_positives / (possible_positives + K.epsilon())
    return recall

def auc(y_true, y_pred):
    ptas = tf.stack([binary_PTA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)],axis=0)
    pfas = tf.stack([binary_PFA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)],axis=0)
    pfas = tf.concat([tf.ones((1,)) ,pfas],axis=0)
    binSizes = -(pfas[1:]-pfas[:-1])
    s = ptas*binSizes
    return K.sum(s, axis=0)

# PFA, prob false alert for binary classifier
def binary_PFA(y_true, y_pred, threshold=K.variable(value=0.5)):
    y_pred = K.cast(y_pred >= threshold, 'float32')
    # N = total number of negative labels
    N = K.sum(1 - y_true)
    # FP = total number of false alerts, alerts from the negative class labels
    FP = K.sum(y_pred - y_pred * y_true)
    return FP/N

# P_TA prob true alerts for binary classifier
def binary_PTA(y_true, y_pred, threshold=K.variable(value=0.5)):
    y_pred = K.cast(y_pred >= threshold, 'float32')
    # P = total number of positive labels
    P = K.sum(y_true)
    # TP = total number of correct alerts, alerts from the positive class labels
    TP = K.sum(y_pred * y_true)
    return TP/P

def f1_loss(y_true, y_pred):
    #计算tp、tn、fp、fn
    tp = K.sum(K.cast(y_true*y_pred, 'float'), axis=0)
    tn = K.sum(K.cast((1-y_true)*(1-y_pred), 'float'), axis=0)
    fp = K.sum(K.cast((1-y_true)*y_pred, 'float'), axis=0)
    fn = K.sum(K.cast(y_true*(1-y_pred), 'float'), axis=0)
    #percision与recall，这里的K.epsilon代表一个小正数，用来避免分母为零
    p = tp / (tp + fp + K.epsilon())
    r = tp / (tp + fn + K.epsilon())
    #计算f1
    f1 = 1.01 * p * r / (0.01 * p + 1 * r + K.epsilon())
    f1 = tf.where(tf.is_nan(f1), tf.zeros_like(f1), f1)#其实就是把nan换成0
    return 1 - K.mean(f1)

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
            self.data = self.data[self.data['DAYSO']>= 90][self.data['next_date'] == self.data['PRE_DATE']]
            self.data['star'] = self.data['TARGET'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
            #self.data.loc[self.data['PASS_MARK'] >= 9.95,'star'] = 0
        else:
            print("target type must be in ['value','percent']")
        self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','DAYSO','RNG_LO',
                                                               'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                               'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                               'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                               'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]
        self.info['cols'] = self.cols

    def set_train_rng(self, train_start, train_end, test_start, test_end):
        self.TR_RNG = pd.Series(pd.date_range(train_start, train_end, freq='D')).apply(lambda x: str(x)[0:10])
        self.TE_RNG = pd.Series(pd.date_range(test_start, test_end, freq='D')).apply(lambda x: str(x)[0:10])
        self.info['train_rng'] = [train_start,train_end]
        self.info['test_rng'] = [test_start,test_end]

    def prepare_data(self, type = 'date', test_size = 0.2, dev_size = 0.2, random_state=0):
        if self.dev_start is None and type == 'date':
            self.train_rng, self.test_rng = train_test_split_date(self.TR_RNG, test_size)
            self.test_rng, self.dev_rng = train_test_split_date(self.test_rng, dev_size)
            self.X_train, self.Y_train = self.data.loc[self.train_rng][self.cols].fillna(0),self.data.loc[self.train_rng]['star'].fillna(0)
            self.X_test, self.Y_test = self.data.loc[self.test_rng][self.cols].fillna(0),self.data.loc[self.test_rng]['star'].fillna(0)
            self.X_dev, self.Y_dev = self.data.loc[self.dev_rng][self.cols].fillna(0),self.data.loc[self.dev_rng]['star'].fillna(0)
        elif self.dev_start is None and type == 'random':
            self.X_train, self.X_test, self.Y_train, self.Y_test = train_test_split(self.data.loc[self.TR_RNG][self.cols],self.data.loc[self.TR_RNG]['star'], test_size=test_size, random_state=random_state)
            self.X_test, self.X_dev, self.Y_test, self.Y_dev = train_test_split(self.X_test,self.Y_test, test_size=dev_size, random_state=random_state)
        self.X_RNG, self.Y_RNG = self.data.loc[self.TE_RNG][self.cols].fillna(0),self.data.loc[self.TE_RNG]['star'].fillna(0)

    def build_model(self, max_depth = 3, subsample = 0.95, seed=1):
        XGBClassifier(max_depth = max_depth, subsample= subsample,seed=seed)

    def model_running(self):
        self.model.fit(self.X_train,self.Y_train,
                                      eval_metric=list(["auc",'error','map']),
                                      eval_set=[(self.X_test,self.Y_test),
                                                (self.X_dev,self.Y_dev)],
                                      verbose=True)
        y_pred = self.model.predict_classes(self.X_train)
        y_pred_test = self.model.predict_classes(self.X_test)
        y_pred_dev = self.model.predict_classes(self.X_dev)
        y_pred_rng = self.model.predict_classes(self.X_RNG)

        accuracy_train = accuracy_score(self.Y_train,y_pred)
        accuracy_test = accuracy_score(self.Y_test,y_pred_test)
        accuracy_dev = accuracy_score(self.Y_dev,y_pred_dev)
        accuracy_rng = accuracy_score(self.Y_RNG,y_pred_rng)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)

        print("accuracy_test:"+str(accuracy_test)+"; precision_score On test:"+str(precision_score(self.Y_test, y_pred_test)))
        self.test_report = classification_report(self.Y_test,y_pred_test, output_dict=True)
        print(self.test_report)

        print("accuracy_test:"+str(accuracy_dev)+"; precision_score On test:"+str(precision_score(self.Y_dev, y_pred_dev)))
        self.dev_report = classification_report(self.Y_dev,y_pred_dev, output_dict=True)
        print(self.dev_report)

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
    b['y_pred'] = model.predict_classes(train)
    bina = pd.DataFrame(model.predict(train))[[0,1]]
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
