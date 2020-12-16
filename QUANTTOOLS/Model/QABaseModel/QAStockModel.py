import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_realtime,get_quant_data_train,get_quant_data
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAModel import QAModel
from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old,QA_fetch_get_stockcode_real,QA_fetch_stock_all,QA_fetch_code_new,QA_fetch_stock_om_all

class QAStockModel(QAModel):

    def get_data(self, start, end, code=None, block=False, sub_block=False, type ='model', norm_type='normalization'):
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=block,sub_block=sub_block, _from=start, _to=end), ui_log = None)
        self.data = get_quant_data(start, end, code=code, type = type, block = block, sub_block = sub_block, norm_type=norm_type)
        self.info['code'] = code
        self.info['norm_type'] = norm_type
        self.info['block'] = block
        self.info['sub_block'] = sub_block
        QA_util_log_info(self.data.shape)

    def model_predict(self, start, end, code = None, type='crawl'):
        if code is not None:
            self.code = code
        QA_util_log_info('##JOB Got Data by {type}, block: {block}, sub_block: {sub_block} ==== from {_from} to {_to}'.format(type=type, block=self.block,sub_block=self.sub_block, _from=start, _to=end), ui_log = None)
        data = get_quant_data(start, end, code = self.code, type= type,block = self.block, sub_block=self.sub_block, norm_type=self.norm_type)
        print(data)
        print(data[['RNG_90', 'AMT_90', 'MAMT_90', 'NEG90_RATE', 'VR', 'VRSI', 'VRSI_C', 'VSTD', 'BOLL', 'UB', 'LB', 'WIDTH', 'WR', 'MR', 'SR', 'WS', 'MS', 'SS', 'MIKE_WRSC', 'MIKE_WRJC', 'MIKE_WSSC', 'MIKE_WSJC', 'MIKE_TR', 'MIKE_BOLL', 'ASI', 'ASIT', 'OBV', 'OBV_C', 'KDJ_K', 'KDJ_D', 'KDJ_J', 'KDJ_CROSS1', 'KDJ_CROSS2', 'WR1', 'WR2', 'WR_CROSS1', 'WR_CROSS2', 'ROC', 'ROCMA', 'RSI1', 'RSI2', 'RSI3', 'RSI1_C', 'RSI2_C', 'RSI3_C', 'RSI_CROSS1', 'RSI_CROSS2', 'CCI', 'CCI_CROSS1', 'CCI_CROSS2', 'CCI_CROSS3', 'CCI_CROSS4', 'BIAS1', 'BIAS2', 'BIAS3', 'BIAS_CROSS1', 'BIAS_CROSS2', 'OSC', 'MAOSC', 'OSC_CROSS1', 'OSC_CROSS2', 'OSC_CROSS3', 'OSC_CROSS4', 'ADTM', 'MAADTM', 'ADTM_CROSS1', 'ADTM_CROSS2', 'DIF', 'DEA', 'MACD', 'CROSS_JC', 'CROSS_SC', 'MACD_TR', 'DI1', 'DI2', 'ADX', 'ADXR', 'ADX_C', 'DI_M', 'DI_CROSS1', 'DI_CROSS2', 'ADX_CROSS1', 'ADX_CROSS2', 'DDD', 'AMA', 'DMA_CROSS1', 'DMA_CROSS2', 'MTM', 'MTMMA', 'MTM_CROSS1', 'MTM_CROSS2', 'MTM_CROSS3', 'MTM_CROSS4', 'MA1', 'MA2', 'MA3', 'MA4', 'BBI', 'MFI', 'MFI_C', 'TR', 'ATR', 'ATRR', 'RSV', 'SKDJ_K', 'SKDJ_D', 'SKDJ_CROSS1', 'SKDJ_CROSS2', 'DDI', 'ADDI', 'AD', 'DDI_C', 'AD_C', 'ADDI_C', 'SHA_LOW', 'SHA_UP', 'BODY', 'BODY_ABS', 'PRICE_PCG', 'MA5', 'MA10', 'MA20', 'MA60', 'SHORT10', 'SHORT20', 'SHORT60', 'LONG60', 'SHORT_CROSS1', 'SHORT_CROSS2', 'LONG_CROSS1', 'LONG_CROSS2', 'LONG_AMOUNT', 'SHORT_AMOUNT', 'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE', 'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK', 'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL', 'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI', 'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE', 'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS', 'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS', 'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM', 'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD', 'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN', 'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE', 'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP', 'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS', 'MA5_C', 'MA10_C', 'MA20_C', 'MA60_C', 'CCI_JC', 'CCI_SC', 'CCI_TR', 'TERNS', 'CCI_TR_HR', 'ALPHA_061', 'ALPHA_123']])
        code_all = QA_fetch_get_stockcode_real(QA_fetch_stock_all().code.unique().tolist())
        code_old = QA_fetch_code_old().code.unique().tolist()
        code_new = QA_fetch_code_new().code.unique().tolist()

        codes = QA_fetch_stock_om_all()
        if self.code is None:
            codes = codes
        else:
            codes = codes[codes.code.isin(self.code)]

        ST = list(codes[codes.name.apply(lambda x:x.count('ST')) == 1]['code']) + list(codes[codes.name.apply(lambda x:x.count('退')) == 1]['code'])
        codes = list(codes['code'])
        code_688 = [i for i in codes if i.startswith('688') == True] + [i for i in codes if i.startswith('787') == True] + [i for i in codes if i.startswith('789') == True]

        target_code = [i for i in code_all if i not in code_new + ST + code_688]

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
        print(train)
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
        print(train)
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

        QA_util_log_info(train.shape[0])
        QA_util_log_info('##JOB Now Got Prediction Result ===== from {_from} to {_to}'.format(_from=start,_to = end), ui_log = None)
        train = train.assign(y_pred = self.model.predict(train[self.cols]))
        bina = pd.DataFrame(self.model.predict_proba(train[self.cols]))[[0,1]]
        bina.index = train.index
        train[['Z_PROB','O_PROB']] = bina
        train.loc[:,'RANK'] = train['O_PROB'].groupby('date').rank(ascending=False)

        if type == 'crawl':
            train = train.join(data[['INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']])
            b = train[['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
        elif type == 'model':
            b = train[['INDUSTRY','y_pred','Z_PROB','O_PROB','RANK']]
        elif type == 'real':
            b = train[['y_pred','Z_PROB','O_PROB','RANK']]

        b = b.join(data[['TERNS']])
        return(b[b.y_pred==1], b)

if __name__ == 'main':
    pass
