from QUANTAXIS.QAUtil import QA_util_date_stamp
import tushare as ts
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_today_str

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
    res = pd.concat([data1,data],axis=1)
    res['crawl_date']=QA_util_today_str()
    return(res.reset_index())

def QA_fetch_get_interest_rate():
    data = get_interest_rate()
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)
