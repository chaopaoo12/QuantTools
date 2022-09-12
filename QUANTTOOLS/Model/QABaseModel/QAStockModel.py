import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_realtime,get_quant_data_train,get_quant_data,get_index_quant_data
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old,QA_fetch_stock_all,QA_fetch_code_new,QA_fetch_stock_om_all
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import normalization, standardize,series_to_supervised
from QUANTAXIS.QAUtil import QA_util_if_trade,QA_util_get_pre_trade_date,QA_util_get_real_date
import re

class QAStockModel(QAModel):

    def get_data(self, start, end, code=None, block=False, sub_block=False, type ='model', norm_type='normalization', ST=True,method='value'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, ST=ST, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data(start, end, code=code, type = type, block = block, sub_block = sub_block, norm_type=norm_type, ST=ST,method=method)
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        QA_util_log_info(self.data.shape)

    def shuffle(self, cols, n_in = None):
        QA_util_log_info('##JOB01 Now Data shuffle {}'.format(n_in))
        if n_in is not None:
            if cols is not None:
                shuffle_data = self.data[cols].groupby('code').apply(series_to_supervised, n_in = n_in)
            else:
                shuffle_data = self.data[[i for i in self.data.columns if i not in ['next_date','OPEN_MARK','PRE_DATE','PASS_MARK','TARGET',
                                                                                    'TARGET3','TARGET4','TARGET5','TARGET10','TARGET20']]].groupby('code').apply(series_to_supervised, n_in = n_in)

            self.data = shuffle_data.join(self.data[['next_date','OPEN_MARK','PRE_DATE','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10','TARGET20']])

        self.info['n_in'] = n_in
        QA_util_log_info('##JOB01 Now Data shuffle Finish')
        QA_util_log_info(self.data.shape)

    def model_predict(self, start, end, code=None, ST=True, type='crawl'):
        self.get_param()

        self.code = code
        QA_util_log_info('##JOB Got Stock Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, ST=ST, _from=start, _to=end, target = self.target), ui_log = None)
        data = get_quant_data(start, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block, ST=ST, norm_type=self.norm_type)
        index_target = get_index_quant_data(start, end,code=['000001','399006'],type='crawl', norm_type=None)
        index_feature = pd.pivot(index_target.loc[(slice(None),['000001','399006']),['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK']].reset_index(),index='date',columns='code',values=['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK'])
        index_feature.columns= ['SKDJ_K1','SKDJ_K6','SKDJ_TR1','SKDJ_TR6','SKDJ_K_WK1','SKDJ_K_WK6','SKDJ_TR_WK1','SKDJ_TR_WK6']
        data = data.join(index_feature)
        code_all = QA_fetch_stock_all()['code'].unique().tolist()
        code_old = QA_fetch_code_old()['code'].unique().tolist()
        code_new = QA_fetch_code_new()['code'].unique().tolist()
        codes = QA_fetch_stock_om_all()
        if self.code is None:
            codes = codes
        else:
            codes = codes[codes.code.isin(self.code)]

        ST = list(codes[codes.name.apply(lambda x:x.count('ST')) == 1]['code']) + list(codes[codes.name.apply(lambda x:x.count('退')) == 1]['code'])
        codes = list(codes['code'])
        code_688 = [i for i in codes if i.startswith('688') == True] + [i for i in codes if i.startswith('787') == True] + [i for i in codes if i.startswith('789') == True]

        try:
            short_of_code = [i for i in code_all if i not in code_old + code_new]
            if len(short_of_code) > 0:
                QA_util_log_info('##JOB {} Short of Code: {} ===== from {_from} to {_to}'.format(len(short_of_code), short_of_code,_from=start,_to = end), ui_log = None)
                send_actionnotice('股票列表数据缺失',
                                  '缺失警告:{}'.format(end),
                                  "缺少股票".format(len(short_of_code)),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )
        except:
            pass

        try:
            target_code = [i for i in code_all if i not in code_new + ST + code_688]
            short_of_data = [i for i in target_code if i not in data.loc[QA_util_get_real_date(end)].reset_index().code.unique().tolist()]

            if len(short_of_data) > 0:
                QA_util_log_info('##JOB {} Short of Data: {} ===== from {_from} to {_to}'.format(len(short_of_data), short_of_data,_from=start,_to = end), ui_log = None)
                send_actionnotice('基础数据缺失',
                                  '缺失警告:{}'.format(end),
                                  "缺少数量".format(len(short_of_data)),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )
        except:
            pass

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        if self.n_in is not None:

            shuffle_data = data[list(set([re.sub(r"\((.*?)\)|\{(.*?)\}|\[(.*?)\]", "", i) for i in self.cols]))].groupby('code').apply(series_to_supervised, n_in = self.n_in)

            data = shuffle_data.join(data)

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

        try:
            target_code = [i for i in code_all if i not in code_new + ST + code_688]
            short_of_data = [i for i in target_code if i not in train.loc[QA_util_get_real_date(end)].reset_index().code.unique().tolist()]

            if len(short_of_data) > 0:
                QA_util_log_info('##JOB {} Short of Data: {} ===== from {_from} to {_to}'.format(len(short_of_data), short_of_data,_from=start,_to = end), ui_log = None)
                send_actionnotice('基础数据缺失',
                                  '缺失警告:{}'.format(end),
                                  "缺少数量".format(len(short_of_data)),
                                  direction = 'WARNING',
                                  offset='WARNING',
                                  volume=None
                                  )
        except:
            pass

        QA_util_log_info(train.shape[0])
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        try:
            norm = self.normoalize
            if norm == 'normal':
                train = train.assign(y_pred = self.model.predict(train[self.cols].groupby('date').apply(normalization)))
                bina = pd.DataFrame(self.model.predict_proba(train[self.cols].groupby('date').apply(normalization)))[[0,1]]
            elif norm == 'stand':
                train = train.assign(y_pred = self.model.predict(train[self.cols].groupby('date').apply(standardize)))
                bina = pd.DataFrame(self.model.predict_proba(train[self.cols].groupby('date').apply(standardize)))[[0,1]]
            else:
                train = train.assign(y_pred = self.model.predict(train[self.cols]))
                bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        except:
            pass

        bina.index = train.index
        train[['Z_PROB','O_PROB']] = bina
        train.loc[:,'RANK'] = train['O_PROB'].groupby('date').rank(ascending=False)
        train = train[train['O_PROB'].notna()]

        if type == 'crawl':
            train = train.join(data[['INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']])
            b = train[['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
        elif type == 'model':
            b = train[['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK']]
        elif type == 'real':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]

        b = b.join(data[['SKDJ_TR','SKDJ_K','SKDJ_TR_HR','SKDJ_K_HR','SKDJ_TR_WK','SKDJ_K_WK','ATRR','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR','RSI3','RSI2','RSI3_C','RSI2_C','RSI3_HR','RSI2_HR','RSI3_C_HR','RSI2_C_HR']])
        return(b[b.y_pred==1], b)

if __name__ == 'main':
    pass
