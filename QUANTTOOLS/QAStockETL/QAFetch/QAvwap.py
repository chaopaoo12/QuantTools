from QUANTTOOLS.QAStockETL.QAFetch.QAUsFinancial import QA_fetch_get_usstock_day_xq, QA_fetch_get_stock_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_get_pre_trade_date,QA_util_log_info,QA_util_get_trade_range
from scipy import stats
import pandas as pd
import math

def rolling_ols(y):
    '''
    滚动回归，返回滚动回归后的回归系数
    rb: 因变量序列
    '''
    #y = pd.DataFrame.ewm(y,alpha=1.0/24,ignore_na=True).mean().values
    model = stats.linregress(y=y, x=pd.Series(range(1,len(y)+1)))
    return(math.atan(model.slope)*180/math.pi)

def spc(data, N= 5):
    data[['vamp_c']]= data.rolling(window=N).agg({'vamp':rolling_ols})
    return(data)

def QA_fetch_get_stock_vwap(code, start_date, end_date, period = '1'):
    try:
        QA_util_log_info("JOB Get {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = QA_fetch_get_stock_min_sina(code, period=period, type='qfq').reset_index(drop=True).set_index(['datetime','code']).drop(columns=['date_stamp'])
        data = data.assign(date=data.reset_index().datetime.apply(lambda x:str(x)[0:10]).tolist(),
                           amt=((data['high'] +data['low']) / 2) * data['volume'])

        data = data.assign(camt = data.groupby('date')['amt'].cumsum(),
                           cvolume = data.groupby('date')['volume'].cumsum())
        data['vamp'] = data['camt'] / data['cvolume']
        data['vamp_c'] = data.groupby('date').rolling(window=5).agg({'vamp':rolling_ols}).reset_index(level=0,drop=True)
    except:
        QA_util_log_info("JOB No {} Minly data for {code} ======= from {start_date} to {end_date}".format(period, code=code, start_date=start_date,end_date=end_date))
        data = None

    return(data)
