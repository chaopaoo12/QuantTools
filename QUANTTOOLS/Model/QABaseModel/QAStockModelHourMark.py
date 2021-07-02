import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_hour,get_hedge_data_realtime
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTTOOLS.Message import send_email, send_actionnotice

class QAStockModelHourMark(QAModel):

    def get_data(self, start, end, code =None, block=False, sub_block=False, type ='model', norm_type='normalization'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data_hour(start, end, code=code, type = type, block = block, sub_block = sub_block, norm_type=norm_type)
        self.data = self.data[(self.data.SKDJ_CROSS2_HR == 1)|(self.data.CROSS_JC_HR == 1)]
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        print(self.data.shape)

    def set_target(self, col, mark, type = 'value', shift= None):
        self.target = col
        QA_util_log_info('##JOB Set Train Target by {type} at {mark} in column {col} ==== {date}'.format(type=type, mark= mark,
                                                                                                         col=col, date= self.info['date']),
                         ui_log = None)

        if isinstance(self.target, list):
            self.data['star'] = self.data[self.target[0]] + self.data[self.target[1]]
        else:
            self.data['star'] = self.data[self.target]

        if type == 'value':
            if shift is not None:
                self.data['star'] = self.data['star'].apply(lambda x: 1 if x >= mark else 0)
            else:
                self.data['star'] = self.data['star'].apply(lambda x: 1 if x >= mark else 0)
        elif type == 'percent':
            self.data['star'] = self.data['star'].groupby('date').apply(lambda x: x.rank(ascending=False, pct=True)).apply(lambda x :1 if x <= mark else 0)
        else:
            QA_util_log_info('##target type must be in [value,percent] ===== {}'.format(self.info['date']), ui_log = None)

        if shift is not None:
            self.data['star'] = self.data['star'].groupby('code').shift(shift)

        self.info['target'] = self.target
        QA_util_log_info('##save used columns ==== {}'.format(self.info['date']), ui_log = None)

    def model_predict(self, start, end, code = None, type='crawl'):
        if code is not None:
            self.code = code
        QA_util_log_info('##JOB Got Stock Quant hour Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, _from=start, _to=end, target = self.target), ui_log = None)
        data = get_quant_data_hour(start, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block, norm_type=self.norm_type)
        data = data[(data.SKDJ_CROSS2_HR == 1)|(data.CROSS_JC_HR == 1)]
        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        cols1 = [i for i in data.columns if i not in [ 'moon','star','mars','venus','sun','MARK','date','datetime',
                                                       'OPEN_MARK','PASS_MARK','TARGET','TARGET3',
                                                       'TARGET4','TARGET5','TARGET10','TARGET20','AVG_TARGET','INDEX_TARGET',
                                                       'INDUSTRY','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5',
                                                       'INDEX_TARGET10','INDEX_TARGET20','date_stamp','PRE_DATE','next_date']]
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
        QA_util_log_info(train.shape[0])

        s_res = train[self.cols].describe().T
        s_res = s_res.assign(rate = s_res['count']/train.shape[0])
        non_cols = list(s_res[s_res.rate < self.drop].index)
        QA_util_log_info([i for i in non_cols if i in self.cols])

        if self.thresh is None:
            train = train[self.cols]
        else:
            nan_num = (train[self.cols].isnull().sum(axis=1)> 0).sum()
            QA_util_log_info('##JOB Clean Data With {NAN_NUM}({per}) in {shape} Contain NAN ==== from {_from} to {_to}'.format(
                NAN_NUM = nan_num, per=nan_num/train.shape[0], shape=train.shape[0], _from=start,_to = end), ui_log = None)

            send_email('模型训练报告:{}'.format(end) + end, "数据损失比例 {}".format(nan_num/train.shape[0]), self.info['date'])
            if nan_num/train.shape[0] >= 0.01:
                send_actionnotice('模型训练报告',
                                  '交易报告:{}'.format(end),
                                  "数据损失比例过高 {}".format(nan_num/train.shape[0]),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )

            if self.thresh == 0:
                train = train[self.cols].dropna()
            else:
                train = train[self.cols].dropna(thresh=(len(self.cols) - self.thresh))

        QA_util_log_info(train.shape[0])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        train = train.assign(y_pred = self.model.predict(train[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        bina.index = train.index
        train[['Z_PROB','O_PROB']] = bina
        train.loc[:,'RANK'] = train['O_PROB'].groupby('datetime').rank(ascending=False)
        train = train[train['O_PROB'].notna()]

        if type == 'crawl':
            train = train.join(data[['PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']])
            b = train[['y_pred','Z_PROB','O_PROB','RANK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
        elif type == 'model':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]
        elif type == 'real':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]

        b = b.join(data[['SKDJ_TR_HR','SKDJ_TR_HR','SKDJ_CROSS1_HR','CROSS_JC_HR','CROSS_SC_HR','SKDJ_CROSS2_HR','MA5_HR','MA10_HR','MA60_HR','CCI_HR','CCI_CROSS1_HR','CCI_CROSS2_HR']])
        return(b[b.y_pred==1], b)

if __name__ == 'main':
    pass
