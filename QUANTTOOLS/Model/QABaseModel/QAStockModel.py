import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_realtime,get_quant_data_train,get_quant_data,get_index_quant_data
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_pre_trade_date,QA_util_get_real_date,QA_util_get_trade_range

class QAStockModel(QAModel):

    def get_data(self, start, end, code=None, block=False, sub_block=False, type ='model', norm_type='normalization', ST=True, method='value'):
        start = QA_util_get_last_day(QA_util_get_real_date(start), 30)
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, ST=ST, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data(start, end, code=code, type = type, block = block, sub_block = sub_block, norm_type=norm_type, ST=ST, method=method)
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        QA_util_log_info(self.data.shape)

    def model_predict(self, start, end, code=None, ST=True, type='crawl'):

        self.get_param()
        self.trading_date = QA_util_get_real_date(end)
        rng = QA_util_get_trade_range(start,end)
        self.code = code
        QA_util_log_info('##JOB Got Stock Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, ST=ST, _from=start, _to=end, target = self.target), ui_log = None)
        if self.n_in is not None:
            start_date = QA_util_get_pre_trade_date(start, max(self.n_in)+1)
        else:
            start_date = start

        self.get_data(start_date, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block,ST=ST, norm_type=self.norm_type)
        index_target = get_index_quant_data(start, end,code=['000001','399006'],type='crawl', norm_type=None)
        index_feature = pd.pivot(index_target.loc[(slice(None),['000001','399006']),['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK']].reset_index(),index='date',columns='code',values=['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK'])
        index_feature.columns= ['SKDJ_K1','SKDJ_K6','SKDJ_TR1','SKDJ_TR6','SKDJ_K_WK1','SKDJ_K_WK6','SKDJ_TR_WK1','SKDJ_TR_WK6']
        self.data = self.data.join(index_feature)

        short_of_code, short_of_data = self.code_check()

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)

        self.shuffle()
        self.data = self.data.loc[rng]
        QA_util_log_info(self.data.shape)
        n_cols = self.data_reshape()
        QA_util_log_info(self.data.shape)
        QA_util_log_info(n_cols)
        QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)

        non_cols,std_cols = self.desribute_check()
        print('desribute_check')
        QA_util_log_info(self.data.shape)
        QA_util_log_info([i for i in non_cols if i in self.cols])

        #loss_rate = self.thresh_check()
        #print('thresh_check')
        QA_util_log_info(self.data.shape)

        QA_util_log_info(self.data.shape[0])
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        self.base_predict()
        self.data.loc[:,'RANK'] = self.data['O_PROB'].groupby('date').rank(ascending=False)
        selec_col = ['SKDJ_TR','SKDJ_K','SKDJ_TR_HR','SKDJ_K_HR','SKDJ_TR_WK',
                     'SKDJ_K_WK','ATRR','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR',
                     'RSI3','RSI2','RSI3_C','RSI2_C','RSI3_HR','RSI2_HR','RSI3_C_HR','RSI2_C_HR','TOTAL_MARKET','PB',
                     'AVG5_TOR','AVG60_TOR']

        selec_col = [i for i in selec_col if i in self.data.columns]
        if type == 'crawl':
            selec_col = selec_col + ['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK',
                                     'PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']
        elif type == 'model':
            selec_col = selec_col + ['INDUSTRY','RANK','y_pred','Z_PROB','O_PROB']
        elif type == 'real':
            selec_col = selec_col + ['y_pred','Z_PROB','O_PROB']

        return(self.data[self.data.y_pred==1][selec_col], self.data[selec_col])


if __name__ == 'main':
    pass
