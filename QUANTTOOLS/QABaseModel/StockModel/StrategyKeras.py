import pandas as pd
from sklearn.metrics import (accuracy_score,
                             classification_report,
                             precision_score)
from QUANTTOOLS.FactorTools.QuantMk import get_quant_data_norm
from QUANTAXIS.QAUtil import (QA_util_log_info)

from keras.layers.normalization import BatchNormalization
import numpy as np
from keras.layers.core import Dense, Dropout, Activation
from keras.optimizers import Adam
from keras.models import Sequential
from keras import backend as K
from QUANTTOOLS.QABaseModel.QAStockModel import QAStockModel

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

class QAStockKeras(QAStockModel):

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

    def model_important(self):
        QA_util_log_info('##JOB Now Got Model Importance ===== {}'.format(self.info['date']), ui_log = None)
        importance = pd.DataFrame({'featur' :list(np.asarray(self.info['cols'])[np.asarray(self.info['fs'].get_support())]),
                                   'value':list(self.model.feature_importances_)}).sort_values(by='value',ascending=False)
        return(importance)

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

        nan_num = train[self.cols].isnull().sum(axis=1)[train[self.cols].isnull().sum(axis=1) == self.thresh].sum()
        QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ==== from {_from} to {_to}'.format(
            NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], thresh=self.thresh,_from=start,_to = end), ui_log = None)

        if self.thresh > 0:
            train = train[self.cols].dropna(thresh=(len(self.cols) - self.thresh)).join(
                train[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict(train[self.cols]))
        b['O_PROB'] = self.model.predict_proba(train[self.cols])
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

if __name__ == 'main':
    pass
