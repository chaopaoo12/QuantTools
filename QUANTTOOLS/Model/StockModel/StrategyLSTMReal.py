import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_norm
from keras.layers import LSTM
from keras.layers.normalization import BatchNormalization
import numpy as np
from keras.layers.core import Dense, Dropout, Activation
from keras.optimizers import Adam
from keras.models import Sequential
from keras import backend as K
from sklearn.metrics import (accuracy_score,
                             classification_report,
                             precision_score)
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAStockModelReal import QAStockModelReal

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

class QAStockLSTMReal(QAStockModelReal):

    def build_model(self, loss = 'binary_crossentropy', optimizer = Adam(lr=1e-4), metrics = ['accuracy',precision]):
        QA_util_log_info('##JOB Set Model Params ===== {}'.format(self.info['date']), ui_log = None)
        self.train = self.train.reshape(self.train.shape[0],1,len(self.cols))
        self.model = Sequential()
        #model.add(Embedding(12, 128))  #features=12
        self.model.add(LSTM(512,return_sequences=True, input_shape=(1, len(self.cols))))#relu隐藏层激活效果更好
        self.model.add(BatchNormalization())
        self.model.add(Activation('relu')) #以Relu函数为激活函数
        self.model.add(Dropout(0.1))

        self.model.add(LSTM(128,return_sequences=True, input_shape=(1, len(self.cols))))#relu隐藏层激活效果更好
        self.model.add(BatchNormalization())
        self.model.add(Activation('relu')) #以Relu函数为激活函数
        self.model.add(Dropout(0.1))

        self.model.add(Dense(1))
        self.model.add(BatchNormalization())
        self.model.add(Activation('sigmoid'))

        self.model.compile(loss=loss, optimizer=optimizer, metrics=metrics)

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

        if self.thresh is None:
            train = train[self.cols]
        elif self.thresh == 0:
            nan_num = train[self.cols].isnull().sum(axis=1)[train[self.cols].isnull().sum(axis=1) > 0].count()
            QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain NAN ==== from {_from} to {_to}'.format(
                NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], _from=start,_to = end), ui_log = None)
            train = train[self.cols].dropna()
        else:
            nan_num = train[self.cols].isnull().sum(axis=1)[train[self.cols].isnull().sum(axis=1) >= self.thresh].count()
            QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain {thresh} NAN ==== from {_from} to {_to}'.format(
                NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], thresh=self.thresh,_from=start,_to = end), ui_log = None)
            train = train[self.cols].dropna(thresh=(len(self.cols) - self.thresh))

        train = train.join(data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict(train[self.cols].reshape(train.shape[0],1,len(self.cols))))
        b['O_PROB'] = self.model.predict_proba(train[self.cols].reshape(train.shape[0],1,len(self.cols)))
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

if __name__ == 'main':
    pass
