import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_train,get_quant_data_realtime
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old,QA_fetch_get_stockcode_real,QA_fetch_stock_all,QA_fetch_code_new

class QAStockModelReal(QAModel):

    def get_data(self, start, end, block=False, sub_block=False, type ='crawl'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data_train(start, end, type = type, block = block, sub_block = sub_block)
        self.data = self.data[(self.data.next_date == self.data.PRE_DATE)]

    def model_predict(self, start, end, block = False, sub_block= False, type='model'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        data = get_quant_data_realtime(start, end, type= type,block = block, sub_block=sub_block)
        code_all = QA_fetch_get_stockcode_real(QA_fetch_stock_all().code.unique().tolist())
        code_old = QA_fetch_code_old().code.unique().tolist()
        code_new = QA_fetch_code_new().code.unique().tolist()
        target_code = [i for i in code_all if i in code_old + code_new]
        short_of_code = [i for i in code_all if i not in code_old + code_new]
        short_of_data = [i for i in target_code if i not in data.loc[end].reset_index().code.unique().tolist()]

        if len(short_of_code) > 0:
            QA_util_log_info('##JOB {} Short of Code: {} ===== from {_from} to {_to}'.format(len(short_of_code), short_of_code,_from=start,_to = end), ui_log = None)
            send_actionnotice('股票列表数据缺失',
                              '缺失警告:{}'.format(end),
                              "缺少股票".format(len(short_of_code)),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

        if len(short_of_data) > 0:
            QA_util_log_info('##JOB {} Short of Data: {} ===== from {_from} to {_to}'.format(len(short_of_data), short_of_data,_from=start,_to = end), ui_log = None)
            send_actionnotice('基础数据缺失',
                              '缺失警告:{}'.format(end),
                              "缺少数量".format(len(short_of_data)),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

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

            send_email('模型训练报告:{}'.format(end) + end, "数据损失比例 {}".format(nan_num/train.shape[0]), self.info['date'])
            if nan_num/train.shape[0] >= 0.01:
                send_actionnotice('数据缺失报告',
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

        short_of_data = [i for i in target_code if i not in train.loc[end].reset_index().code.unique().tolist()]

        if len(short_of_data) > 0:
            QA_util_log_info('##JOB {} Short of Data: {} ===== from {_from} to {_to}'.format(len(short_of_data), short_of_data,_from=start,_to = end), ui_log = None)
            send_actionnotice('基础数据缺失',
                              '缺失警告:{}'.format(end),
                              "缺少数量".format(len(short_of_data)),
                              direction = 'WARNING',
                              offset='WARNING',
                              volume=None
                              )

        train = train.join(data[['INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']])

        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        b = train[['INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','INDEX_TARGET10']]
        b = b.assign(y_pred = self.model.predict(train[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        bina.index = b.index
        b[['Z_PROB','O_PROB']] = bina
        b.loc[:,'RANK'] = b['O_PROB'].groupby('date').rank(ascending=False)
        return(b[b['y_pred']==1], b)

if __name__ == 'main':
    pass
