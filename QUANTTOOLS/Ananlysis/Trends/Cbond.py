import akshare as ak
import pandas as pd
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data,get_quant_data
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date,QA_util_get_real_date
from QUANTTOOLS.QAStockETL.QAUtil.QADate_trade import (QA_util_get_trade_range)

def CBond(trading_date):
    end_date = trading_date
    start_date = QA_util_get_pre_trade_date(trading_date,14)

    bond_cov_comparison_df = ak.bond_cov_comparison()

    bond_cov_comparison_df[['转债最新价', '转债涨跌幅','正股最新价',
                            '正股涨跌幅', '转股价', '转股价值', '转股溢价率', '纯债溢价率', '回售触发价', '强赎触发价', '到期赎回价',
                            '纯债价值','上市日期']] = bond_cov_comparison_df[['转债最新价', '转债涨跌幅','正股最新价',
                                                                      '正股涨跌幅', '转股价', '转股价值', '转股溢价率', '纯债溢价率', '回售触发价', '强赎触发价', '到期赎回价',
                                                                      '纯债价值','上市日期']].apply(lambda x: x.replace('-',0))

    bond_cov_comparison_df[['转债最新价', '转债涨跌幅','正股最新价',
                            '正股涨跌幅', '转股价', '转股价值', '转股溢价率', '纯债溢价率', '回售触发价', '强赎触发价', '到期赎回价',
                            '纯债价值','上市日期']] = bond_cov_comparison_df[['转债最新价', '转债涨跌幅','正股最新价',
                                                                      '正股涨跌幅', '转股价', '转股价值', '转股溢价率', '纯债溢价率', '回售触发价', '强赎触发价', '到期赎回价',
                                                                      '纯债价值','上市日期']].apply(pd.to_numeric)

    res = []
    for i in bond_cov_comparison_df[bond_cov_comparison_df['转债最新价'] > 0]['转债代码'].tolist():
        bond_zh_cov_value_analysis_df = ak.bond_zh_cov_value_analysis(symbol=i)
        bond_zh_cov_value_analysis_df = bond_zh_cov_value_analysis_df.assign(cbond = i)
        res.append(bond_zh_cov_value_analysis_df)
    res = pd.concat(res)
    res['日期'] = pd.to_datetime(res['日期'])

    date_rng = QA_util_get_trade_range(start_date, end_date)

    date_res = []
    for i in date_rng:
        res_a = res[res['日期'] < i].groupby('cbond')[['转股溢价率','纯债溢价率']].describe()
        res_a = res_a.assign(date = i)
        date_res.append(res_a)
    date_res = pd.concat(date_res)
    date_res.columns = ['stock_cnt','stock_mean','stock_std','stock_min','stock_25','stock_50','stock_75','stock_max','bond_cnt','bond_mean','bond_std','bond_min','bond_25','bond_50','bond_75','bond_max','date']
    date_res['date'] = pd.to_datetime(date_res['date'])

    date_res = date_res.reset_index().set_index(['date','cbond']).join(res.rename(columns={'日期':'date'}).set_index(['date','cbond']))
    df = date_res.reset_index().set_index(['cbond']).join(bond_cov_comparison_df.rename(columns={'转债代码':'cbond','正股代码':'code'}).set_index('cbond')[['转债名称','code','正股名称','转股价','回售触发价','强赎触发价','到期赎回价','开始转股日','上市日期']]).reset_index().set_index(['date','cbond'])
    df = df.assign(bond_gap = df['纯债溢价率'] - df['bond_50'],
                   stock_gap = df['转股溢价率'] - df['stock_50'])

    stock_target = get_quant_data(start_date, end_date, code=bond_cov_comparison_df[bond_cov_comparison_df['转债最新价'] > 0]['正股代码'].tolist(), type='crawl', block=False, sub_block=False,norm_type=None)
    stock_res = stock_target[['RRNG','RRNG_HR','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']].reset_index()
    stock_res['date'] = pd.to_datetime(stock_res['date'])

    df1 = df.reset_index().set_index(['date','code']).join(stock_res.set_index(['date','code'])).reset_index().set_index(['date','cbond'])
    df1 = df1[df1.RRNG.abs() < 0.05]
    df1['RANK'] = df1['stock_gap'].groupby('date').rank()
    return(df1)