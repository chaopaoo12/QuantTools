import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import normalization, standardize,series_to_supervised
import re

class QAIndexModel(QAModel):

    def get_data(self, start, end, code=None, type ='model', norm_type=None):
        QA_util_log_info('##JOB Got Index Data by {type} ==== from {_from} to {_to}'.format(type=type, _from=start, _to=end), ui_log = None)
        self.data = get_index_quant_data(start, end, code=code, type = type, norm_type=norm_type)
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        print(self.data.shape)

    def model_predict(self, start, end, code = None, type='crawl'):
        if code is not None:
            self.code = code
        QA_util_log_info('##JOB Got Index Quant Data by {type} ==== from {_from} to {_to} target:{target}'.format(type=type, _from=start, _to=end, target = self.target), ui_log = None)
        data = get_index_quant_data(start, end, code = self.code, type= type, norm_type=self.norm_type)

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        cols1 = [i for i in data.columns if i not in [ 'moon','star','mars','venus','sun','MARK','date','datetime','TARGET20',
                                                       'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                       'TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET',
                                                       'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                       'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]

        if self.n_in is not None:

            shuffle_data = data[list(set([re.sub(r"\((.*?)\)|\{(.*?)\}|\[(.*?)\]", "", i) for i in self.cols]))].groupby('code').apply(series_to_supervised, n_in = self.n_in)

            data = shuffle_data.join(data)

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
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        train = train.assign(y_pred = self.model.predict(train[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        bina.index = train.index
        train[['Z_PROB','O_PROB']] = bina
        train.loc[:,'RANK'] = train['O_PROB'].groupby('date').rank(ascending=False)
        train = train[train['O_PROB'].notna()]

        if type == 'crawl':
            train = train.join(data[['PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])
            b = train[['y_pred','Z_PROB','O_PROB','RANK','PASS_MARK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        elif type == 'model':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]
        elif type == 'real':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]

        b = b.join(data[['SKDJ_TR_HR','SKDJ_K_HR','SKDJ_TR','SKDJ_K','SKDJ_CROSS2','SKDJ_CROSS1','CROSS_JC','SKDJ_TR_WK','SKDJ_K_WK','ATRR','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR','RSI3','RSI2','RSI3_C','RSI2_C','RSI3_HR','RSI2_HR','RSI3_C_HR','RSI2_C_HR']])
        return(b[b.y_pred==1], b)

if __name__ == 'main':
    pass