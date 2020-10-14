import pandas as pd
from sklearn.metrics import (accuracy_score,classification_report)
from QUANTAXIS.QAUtil import QA_util_log_info
from keras.layers.normalization import BatchNormalization
from sklearn.metrics import f1_score, precision_score, recall_score
from keras.optimizers import Adam
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras import backend as K
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.Model.QABaseModel.QAIndexModel import QAIndexModel
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

class QAIndexKeras(QAIndexModel):

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

    def model_running(self,batch_size=4096,nb_epoch=100,validation_split=0.2):
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
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        self.info['train_report'] = self.train_report

    def model_predict(self, start, end, type='crawl'):
        QA_util_log_info('##JOB Got Index Data by {type} ==== from {_from} to {_to}'.format(type=type, _from=start, _to=end), ui_log = None)
        data = get_index_quant_data(start, end, type= type)

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

        train = train.join(data[['INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict_classes(train))
        b['O_PROB'] = pd.DataFrame(self.model.predict_proba(train))
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

