import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_realtime,get_quant_data_train,get_quant_data,get_index_quant_data
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTAXIS.QAUtil import QA_util_if_trade,QA_util_get_pre_trade_date,QA_util_get_real_date

class QAStockModel(QAModel):

    def get_data(self, start, end, code=None, block=False, sub_block=False, type ='model', norm_type='normalization', ST=True,method='value'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, ST=ST, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data(start, end, code=code, type = type, block = block, sub_block = sub_block, norm_type=norm_type, ST=ST,method=method)
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        QA_util_log_info(self.data.shape)

    def model_predict(self, start, end, code=None, ST=True, type='crawl'):
        self.get_param()
        self.trading_date = QA_util_get_real_date(end)

        self.code = code
        QA_util_log_info('##JOB Got Stock Data by {type}, block: {block}, sub_block: {sub_block}, ST: {ST} ==== from {_from} to {_to} target:{target}'.format(type=type, block=self.block,sub_block=self.sub_block, ST=ST, _from=start, _to=end, target = self.target), ui_log = None)
        self.data = get_quant_data(start, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block, ST=ST, norm_type=self.norm_type)

        index_target = get_index_quant_data(start, end,code=['000001','399006'],type='crawl', norm_type=None)
        index_feature = pd.pivot(index_target.loc[(slice(None),['000001','399006']),['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK']].reset_index(),index='date',columns='code',values=['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK'])
        index_feature.columns= ['SKDJ_K1','SKDJ_K6','SKDJ_TR1','SKDJ_TR6','SKDJ_K_WK1','SKDJ_K_WK6','SKDJ_TR_WK1','SKDJ_TR_WK6']
        self.data = self.data.join(index_feature)

        short_of_code, short_of_data = self.code_check()

        QA_util_log_info('##JOB Now Reshape Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)

        self.shuffle()
        QA_util_log_info(self.data.shape)
        n_cols = self.data_reshape()
        QA_util_log_info(self.data.shape)
        QA_util_log_info(n_cols)
        QA_util_log_info('##JOB Now Got Different Columns ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)

        non_cols = self.desribute_check()
        print('desribute_check')
        QA_util_log_info(self.data.shape)
        QA_util_log_info([i for i in non_cols if i in self.cols])
        ccc= ['ATRR(t-10)', 'AVG30_RNG(t-10)', 'AVG20_RNG(t-10)', 'AVG60_RNG(t-10)', 'ALPHA_154(t-10)', 'ALPHA_024(t-10)', 'WR(t-10)', 'ASIT(t-10)', 'ALPHA_021(t-10)', 'ALPHA_056(t-10)', 'LONG_TR(t-10)', 'AVG60_TR(t-10)', 'MA_VOL45(t-10)', 'ALPHA_006(t-10)', 'AVG60_C_MARKET(t-10)', 'ALPHA_041(t-10)', 'ALPHA_082(t-10)', 'MA30(t-10)', 'ALPHA_057(t-10)', 'MA_VOL60(t-10)', 'SHORT_AMOUNT(t-10)', 'KDJ_K(t-10)', 'PB_60DN(t-10)', 'ALPHA_003(t-10)', 'ALPHA_042(t-10)', 'MA30_C(t-10)', 'ALPHA_081(t-10)', 'ALPHA_067(t-10)', 'SHORT20(t-10)', 'ALPHA_096(t-10)', 'LONG_AMOUNT(t-10)', 'ALPHA_097(t-10)', 'MA60(t-10)', 'ALPHA_170(t-10)', 'WR1(t-10)', 'MA_VOL30(t-10)', 'GMMA40(t-10)', 'ALPHA_035(t-10)', 'RSI2(t-10)', 'MA_VOL35(t-10)', 'AVG5_TOR(t-10)', 'ALPHA_076(t-10)', 'MA_VOL50(t-10)', 'MA_VOL20(t-10)', 'RNG_90(t-10)', 'PE_90UP(t-10)', 'ALPHA_100(t-10)', 'ALPHA_070(t-10)', 'ALPHA_098(t-10)', 'AVG20(t-10)', 'ATRR(t-15)', 'AVG30_RNG(t-15)', 'AVG20_RNG(t-15)', 'AVG60_RNG(t-15)', 'ALPHA_154(t-15)', 'ALPHA_024(t-15)', 'WR(t-15)', 'ASIT(t-15)', 'ALPHA_021(t-15)', 'ALPHA_056(t-15)', 'LONG_TR(t-15)', 'AVG60_TR(t-15)', 'MA_VOL45(t-15)', 'ALPHA_006(t-15)', 'AVG60_C_MARKET(t-15)', 'ALPHA_041(t-15)', 'ALPHA_082(t-15)', 'MA30(t-15)', 'ALPHA_057(t-15)', 'MA_VOL60(t-15)', 'SHORT_AMOUNT(t-15)', 'KDJ_K(t-15)', 'PB_60DN(t-15)', 'ALPHA_003(t-15)', 'ALPHA_042(t-15)', 'MA30_C(t-15)', 'ALPHA_081(t-15)', 'ALPHA_067(t-15)', 'SHORT20(t-15)', 'ALPHA_096(t-15)', 'LONG_AMOUNT(t-15)', 'ALPHA_097(t-15)', 'MA60(t-15)', 'ALPHA_170(t-15)', 'WR1(t-15)', 'MA_VOL30(t-15)', 'GMMA40(t-15)', 'ALPHA_035(t-15)', 'RSI2(t-15)', 'MA_VOL35(t-15)', 'AVG5_TOR(t-15)', 'ALPHA_076(t-15)', 'MA_VOL50(t-15)', 'MA_VOL20(t-15)', 'RNG_90(t-15)', 'PE_90UP(t-15)', 'ALPHA_100(t-15)', 'ALPHA_070(t-15)', 'ALPHA_098(t-15)', 'AVG20(t-15)', 'ATRR(t-20)', 'AVG30_RNG(t-20)', 'AVG20_RNG(t-20)', 'AVG60_RNG(t-20)', 'ALPHA_154(t-20)', 'ALPHA_024(t-20)', 'WR(t-20)', 'ASIT(t-20)', 'ALPHA_021(t-20)', 'ALPHA_056(t-20)', 'LONG_TR(t-20)', 'AVG60_TR(t-20)', 'MA_VOL45(t-20)', 'ALPHA_006(t-20)', 'AVG60_C_MARKET(t-20)', 'ALPHA_041(t-20)', 'ALPHA_082(t-20)', 'MA30(t-20)', 'ALPHA_057(t-20)', 'MA_VOL60(t-20)', 'SHORT_AMOUNT(t-20)', 'KDJ_K(t-20)', 'PB_60DN(t-20)', 'ALPHA_003(t-20)', 'ALPHA_042(t-20)', 'MA30_C(t-20)', 'ALPHA_081(t-20)', 'ALPHA_067(t-20)', 'SHORT20(t-20)', 'ALPHA_096(t-20)', 'LONG_AMOUNT(t-20)', 'ALPHA_097(t-20)', 'MA60(t-20)', 'ALPHA_170(t-20)', 'WR1(t-20)', 'MA_VOL30(t-20)', 'GMMA40(t-20)', 'ALPHA_035(t-20)', 'RSI2(t-20)', 'MA_VOL35(t-20)', 'AVG5_TOR(t-20)', 'ALPHA_076(t-20)', 'MA_VOL50(t-20)', 'MA_VOL20(t-20)', 'RNG_90(t-20)', 'PE_90UP(t-20)', 'ALPHA_100(t-20)', 'ALPHA_070(t-20)', 'ALPHA_098(t-20)', 'AVG20(t-20)']

        print(self.data[ccc])
        loss_rate = self.thresh_check()
        print('thresh_check')
        QA_util_log_info(self.data.shape)

        QA_util_log_info(self.data.shape[0])
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        self.base_predict()
        self.data.loc[:,'RANK'] = self.data['O_PROB'].groupby('date').rank(ascending=False)
        selec_col = ['SKDJ_TR','SKDJ_K','SKDJ_TR_HR','SKDJ_K_HR','SKDJ_TR_WK',
                     'SKDJ_K_WK','ATRR','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR',
                     'RSI3','RSI2','RSI3_C','RSI2_C','RSI3_HR','RSI2_HR','RSI3_C_HR','RSI2_C_HR']

        selec_col = [i for i in selec_col if i in self.data.columns]
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
