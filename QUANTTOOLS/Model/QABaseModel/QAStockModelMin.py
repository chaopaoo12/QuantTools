from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_15min
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTAXIS.QAUtil import QA_util_if_trade,QA_util_get_pre_trade_date,QA_util_get_real_date
import re
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_vwap
from QUANTTOOLS.QAStockETL.FuncTools.TransForm import normalization, standardize,series_to_supervised


class QAStockModelMin(QAModel):

    def get_data(self, start, end, code =None, block=False, sub_block=False, type = None, norm_type=None, ST=True):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        data = QA_fetch_get_stock_vwap(code, start, end, period = '1', type = 'crawl')
        data_15min = get_quant_data_15min(start, end, code, type='real')
        last = data.groupby(['date','code'])['close'].agg([('last1','last')])
        last = last.assign(yes_close=last.groupby('code').shift())

        data = last.join(data.reset_index().set_index(['date','code'])).reset_index().set_index(['datetime','code'])
        data = data.assign(TARGET = data.day_close/data.last1-1,
                           pct= data.day_close/data.yes_close-1)
        self.data = data[['date','open_pct', 'high_pct', 'low_pct', 'VAMP_JC', 'VAMP_SC', 'VAMPC_K', 'VAMP_K', 'CLOSE_K', 'TARGET', 'pct', 'AMT_UP', 'DISTANCE', 'camt_vol', 'camt_k']].join(data_15min)
        self.data = self.data.groupby('code').fillna(method='ffill')

        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        print(self.data.shape)

    def model_predict(self, start, end, code = None, type='crawl', ST=True):

        self.get_param()
        self.trading_date = QA_util_get_real_date(end)

        self.code = code
        QA_util_log_info('##JOB Got Stock Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, ST=ST, _from=start, _to=end, target = self.target), ui_log = None)

        self.get_data(start, end, code)
        short_of_code, short_of_data = self.code_check()

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        if self.n_in is not None:

            shuffle_data = self.data[list(set([re.sub(r"\((.*?)\)|\{(.*?)\}|\[(.*?)\]", "", i) for i in self.cols]))].groupby('code').apply(series_to_supervised, n_in = self.n_in)

            self.data = shuffle_data.join(self.data)

        n_cols = self.data_reshape()
        QA_util_log_info(n_cols)
        QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)

        non_cols = self.desribute_check()
        QA_util_log_info([i for i in non_cols if i in self.cols])

        loss_rate = self.thresh_check()

        QA_util_log_info(self.data.shape[0])
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        self.base_predict()

        selec_col = ['SKDJ_TR_15M']
        if type == 'crawl':
            selec_col = selec_col + ['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK',
                                     'PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']
        elif type == 'model':
            selec_col = selec_col + ['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK']
        elif type == 'real':
            pass

        return(self.data[self.data.y_pred==1][selec_col], self.data[selec_col])

if __name__ == 'main':
    pass
