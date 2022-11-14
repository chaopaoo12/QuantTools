import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_realtime,get_quant_data_train,get_quant_data,get_quant_data_neut
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTAXIS.QAUtil import QA_util_if_trade,QA_util_get_pre_trade_date,QA_util_get_real_date,QA_util_get_trade_range

class QAStockModelNeut(QAModel):

    def get_data(self, start, end, code=None, block=False, sub_block=False, type ='crawl', norm_type=None, ST=True,method='value'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, ST=ST, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data_neut(start, end, code=code, type = type, block = block, sub_block = sub_block, ST =ST,method=method)
        self.info['code'] = code
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        QA_util_log_info(self.data.shape)

    def model_predict(self, start, end, code=None, ST=True, type='crawl'):
        self.get_param()
        self.trading_date = QA_util_get_real_date(end)
        rng = QA_util_get_trade_range(start,end)
        self.code = code
        if self.n_in is not None:
            start_date = QA_util_get_pre_trade_date(start, max(self.n_in)+1)
        else:
            start_date = start

        QA_util_log_info('##JOB Got Stock Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, ST=ST, _from=start, _to=end, target = self.target), ui_log = None)
        self.get_data(start_date, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block,ST=ST)

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

        selec_col = []

        selec_col = [i for i in selec_col if i in self.data.columns]
        if type == 'crawl':
            selec_col = selec_col + ['y_pred','Z_PROB','O_PROB','RANK',
                                     'PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']
        elif type == 'model':
            selec_col = selec_col + ['RANK','y_pred','Z_PROB','O_PROB']
        elif type == 'real':
            selec_col = selec_col + ['y_pred','Z_PROB','O_PROB']

        return(self.data[self.data.y_pred==1][selec_col], self.data[selec_col])

if __name__ == 'main':
    pass
