import pandas as pd
from sklearn.metrics import (accuracy_score,confusion_matrix,
                             classification_report,roc_curve,roc_auc_score,
                             auc,precision_score,recall_score,f1_score)
from QUANTTOOLS.FactorTools.QuantMk import get_quant_data_norm
from QUANTAXIS.QAUtil import (QA_util_log_info, QA_util_today_str,QA_util_get_trade_range)
import joblib
from QUANTTOOLS.FactorTools.base_func import mkdir
from keras.layers.normalization import BatchNormalization
import numpy as np
from keras.layers.core import Dense, Dropout, Activation
from keras.optimizers import Adam
from keras.models import Sequential
from sklearn.utils import shuffle
from keras import backend as K

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

class model():

    def __init__(self):
        self.info=dict()
        self.info['date'] = QA_util_today_str()
        self.info['train_status']=dict()
        self.info['rng_status']=dict()

    def get_data(self, start, end, block=True, sub_block=True, type ='crawl'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data_norm(start, end, type = type, block = block, sub_block = sub_block)
        self.data = self.data[(self.data.DAYS>= 90)&(self.data.next_date == self.data.PRE_DATE)]
        print(self.data.shape)

    def set_target(self, mark, type = 'value'):
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} ===== {date}'.format(type = type, mark=mark,
                                                                                          date = self.info['date']),
                         ui_log = None)
        if type == 'value':
            self.data['star'] = self.data['TARGET5'].apply(lambda x : 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data['TARGET5'].groupby('date').apply(lambda x: x.rank(ascending=False,pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            QA_util_log_info('##target type must be in [value,percent] ===== {}'.format(self.info['date']), ui_log = None)

        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)
        self.cols = [i for i in self.data.columns if i not in ['moon','star','mars','venus','sun','MARK','RNG_LO','RNG_L','LAG_TOR',
                                                               'LAG_TORO','OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                               'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                               'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                               'INDEX_TARGET10','date_stamp','PRE_DATE','next_date']]

    def set_train_rng(self, train_start, train_end):
        QA_util_log_info('##JOB Set Train Range from {_from} to {_to} ===== {date}'.format(_from=train_start,_to=train_end, date=self.info['date']), ui_log = None)
        self.TR_RNG = QA_util_get_trade_range(train_start, train_end)
        self.info['train_rng'] = [train_start,train_end]

    def prepare_data(self,thresh = 0, cols= None):
        if cols is None:
            pass
        else:
            self.cols = cols
        nan_num = self.data[self.cols].isnull().sum(axis=1)[self.data[self.cols].isnull().sum(axis=1) == thresh].sum()
        QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ===== {date}'.format(
            NAN_NUM = nan_num, per=nan_num/self.data.shape[0], shape=self.data.shape[0], thresh=thresh,date=self.info['date']), ui_log = None)
        self.data = self.data[self.cols].dropna(thresh=(len(self.cols) - thresh))
        QA_util_log_info('##JOB Split Train Data ===== {}'.format(self.info['date']), ui_log = None)
        self.X_train, self.Y_train = shuffle(self.data.loc[self.TR_RNG][self.cols].fillna(0),self.data.loc[self.TR_RNG]['star'])
        self.info['thresh'] = thresh

    def build_model(self, loss = 'binary_crossentropy', optimizer = Adam(lr=1e-4), metrics = ['accuracy',precision]):
        QA_util_log_info('##JOB Set Model Params ===== {}'.format(self.info['date']), ui_log = None)

        self.model = Sequential() #建立模型

        self.model.add(Dense(input_dim = self.X_train.shape[1], units = 256)) #添加输入层、隐藏层的连接
        self.model.add(BatchNormalization())
        self.model.add(Activation('relu')) #以Relu函数为激活函数
        self.model.add(Dropout(0.2))

        self.model.add(Dense(input_dim = 256, units = 128)) #添加隐藏层、隐藏层的连接
        self.model.add(BatchNormalization())
        self.model.add(Activation('relu')) #以Relu函数为激活函数
        self.model.add(Dropout(0.2))

        self.model.add(Dense(input_dim = 128, units = 1)) #添加隐藏层、输出层的连接
        self.model.add(BatchNormalization())
        self.model.add(Activation('sigmoid')) #以sigmoid函数为激活函数

        #编译模型，损失函数为binary_crossentropy，用adam法求解
        self.model.compile(loss=loss, optimizer=optimizer,metrics=metrics)

    def model_running(self, batch_size=4096,nb_epoch=100,validation_split=0.2):
        QA_util_log_info('##JOB Now Model Traning ===== {}'.format(self.info['date']), ui_log = None)
        #self.model.fit(self.X_train,self.Y_train)
        self.model.fit(self.X_train, self.Y_train,
                       batch_size=batch_size,
                       epochs=nb_epoch,
                       verbose=1,
                       validation_split=validation_split)

        QA_util_log_info('##JOB Now Model Scoring ===== {}'.format(self.info['date']), ui_log = None)
        y_pred = self.model.predict(self.X_train)

        accuracy_train = accuracy_score(self.Y_train,y_pred)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train, y_pred, output_dict=True)
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
        self.info['cols'] = self.cols
        QA_util_log_info('##JOB Now Model Saving ===== {}'.format(self.info['date']), ui_log = None)

        if mkdir(working_dir):
            try:
                joblib.dump(self.model, working_dir+"\\{name}keras.joblib.dat".format(name=name))
                joblib.dump(self.info, working_dir+"\\{name}keras_info.joblib.dat".format(name=name))
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
    model = joblib.load(working_dir+"\\{name}keras.joblib.dat".format(name=name))
    info = joblib.load(working_dir+"\\{name}keras_info.joblib.dat".format(name=name))
    return(model, info)

def model_predict(model, start, end, cols, thresh, block = False, sub_block= False, type='crawl'):
    QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
    data = get_quant_data_norm(start, end, type= type,block = block, sub_block=sub_block)

    QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
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
    QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    QA_util_log_info(n_cols)

    nan_num = train[cols].isnull().sum(axis=1)[train[cols].isnull().sum(axis=1) == thresh].sum()
    QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ==== from {_from} to {_to}'.format(
        NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], thresh=thresh,_from=start,_to = end), ui_log = None)
    train = train[cols].dropna(thresh=(len(cols) - thresh))

    QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
    b = data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
    b = b.assign(y_pred = model.predict(train))
    b['O_PROB'] = model.predict_proba(train)
    b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
    return(b[b['y_pred']==1], b)

def check_model(model, start, end, cols, target, type = 'value',block=False, sub_block=False):
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

if __name__ == 'main':
    pass
