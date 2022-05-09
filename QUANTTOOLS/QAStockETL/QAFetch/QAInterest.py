from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_today_str
import tushare as ts
import pandas as pd
import akshare as ak
import numpy as np
from scipy import stats

def per25(x):
    return(np.percentile(x, 25))

def per75(x):
    return(np.percentile(x, 75))

def perc(x):
    x = list(x)
    tar = x[-1]
    return(stats.percentileofscore(x, tar))

def get_globalinterest_rate():
    macro_bank_usa_interest_rate_df = ak.macro_bank_usa_interest_rate()
    macro_bank_euro_interest_rate_df = ak.macro_bank_euro_interest_rate()
    macro_bank_newzealand_interest_rate_df = ak.macro_bank_newzealand_interest_rate()
    macro_bank_china_interest_rate_df = ak.macro_bank_china_interest_rate()
    macro_bank_switzerland_interest_rate_df = ak.macro_bank_switzerland_interest_rate()
    macro_bank_english_interest_rate_df = ak.macro_bank_english_interest_rate()
    macro_bank_australia_interest_rate_df = ak.macro_bank_australia_interest_rate()
    macro_bank_japan_interest_rate_df = ak.macro_bank_japan_interest_rate()
    macro_bank_russia_interest_rate_df = ak.macro_bank_russia_interest_rate()
    macro_bank_india_interest_rate_df = ak.macro_bank_india_interest_rate()
    macro_bank_brazil_interest_rate_df = ak.macro_bank_brazil_interest_rate()
    res=[]
    for i in [macro_bank_usa_interest_rate_df,macro_bank_euro_interest_rate_df,macro_bank_newzealand_interest_rate_df,
              macro_bank_china_interest_rate_df,macro_bank_switzerland_interest_rate_df,macro_bank_english_interest_rate_df,
              macro_bank_australia_interest_rate_df,macro_bank_japan_interest_rate_df,macro_bank_russia_interest_rate_df,
              macro_bank_india_interest_rate_df,macro_bank_brazil_interest_rate_df]:
        i = i.rename(columns={'商品':'code','日期':'date','今值':'vars'}).dropna(subset=['vars']).set_index(['code','date']).sort_index().reset_index()
        i[['mean','per25','per75','perc']] = i[['vars']].rolling(100, min_periods=10).agg(['mean',per25,per75,perc])
        res.append(i)
    return(pd.concat(res))

def get_interest_rate():
    deposit= ts.get_deposit_rate()
    loan = ts.get_loan_rate()
    data = deposit[[x.startswith('定期存款整存整取') for x in deposit['deposit_type']]].pivot_table(values = "rate",index=['date'], columns='deposit_type', aggfunc=sum)
    data1 = loan[[x.startswith('短期贷款') or x.startswith('中长期贷款') for x in loan['loan_type']]].pivot_table(values = "rate",index=['date'], columns='loan_type', aggfunc=sum)
    data.columns = [x.strip().replace('''定期存款整存整取''','').replace('''(''','').replace(''')''','') for x in data.columns]
    data.columns = ['DOYEAR' if x == '一年' else x for x in data.columns]
    data.columns = ['DTMON' if x == '三个月' else x for x in data.columns]
    data.columns = ['DTYEAR' if x == '三年' else x for x in data.columns]
    data.columns = ['DSYEAR' if x == '二年' else x for x in data.columns]
    data.columns = ['DFYEAR' if x == '五年' else x for x in data.columns]
    data.columns = ['DHYEAR' if x == '半年' else x for x in data.columns]
    data1.columns = [x.strip().replace('''短期贷款''','').replace('''中长期贷款''','').replace('''(''','').replace(''')''','') for x in data1.columns]
    data1.columns = ['SYEAR' if x == '一至三年' else x for x in data1.columns]
    data1.columns = ['LTMON' if x == '三至五年' else x for x in data1.columns]
    data1.columns = ['LFYEAR' if x == '五年以上' else x for x in data1.columns]
    data1.columns = ['LHYEAR' if x == '六个月以内' else x for x in data1.columns]
    data1.columns = ['LOYEAR' if x == '六个月至一年' else x for x in data1.columns]
    res = pd.concat([data1,data],axis=1).reset_index().fillna(method='ffill')
    res.columns = ['date' if x == 'index' else x for x in res.columns]
    res['crawl_date']=QA_util_today_str()
    return(res)

def QA_fetch_get_interest_rate():
    data = get_interest_rate()
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

if __name__ == '__main__':
    pass