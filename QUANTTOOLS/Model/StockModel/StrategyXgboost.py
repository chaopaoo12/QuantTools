import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import (accuracy_score,
                             classification_report,
                             precision_score)
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_norm
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAStockModel import QAStockModel
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice

class QAStockXGBoost(QAStockModel):

    def build_model(self, other_params):
        QA_util_log_info('##JOB Set Model Params ===== {}'.format(self.info['date']), ui_log = None)
        self.info['other_params'] = other_params
        self.model = XGBClassifier(**self.info['other_params'])

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

    def model_important(self):
        QA_util_log_info('##JOB Now Got Model Importance ===== {}'.format(self.info['date']), ui_log = None)
        self.info['importance'] = pd.DataFrame({'featur' :self.info['cols'],'value':list(self.model.feature_importances_)}
                                  ).sort_values(by='value',ascending=False)
        return(self.info['importance'])


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

            send_email('模型训练报告:'+ end, "数据损失比例 {}".format(nan_num/self.data.shape[0]), self.info['date'])
            if nan_num/self.data.shape[0] >= 0.01:
                send_actionnotice('模型训练报告',
                                  '交易报告:{}'.format(end),
                                  "数据损失比例过高 {}".format(nan_num/self.data.shape[0]),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )

        train = train.join(data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict(train[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        bina.index = b.index
        b[['Z_PROB','O_PROB']] = bina
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

if __name__ == 'main':
    pass
