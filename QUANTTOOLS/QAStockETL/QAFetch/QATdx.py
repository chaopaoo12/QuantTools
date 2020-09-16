from QUANTAXIS import QA_fetch_get_future_day
from QUANTTOOLS.QAStockETL.QAData.database_settings import tdx_dir
import easyquotation
import pandas as pd
import akshare as ak

QA_fetch_get_usstock_day = QA_fetch_get_future_day

def QA_fetch_get_stock_realtime(code):
    quotation = easyquotation.use('sina')
    values = pd.DataFrame(quotation.stocks(code)).T
    values.index.name = 'code'
    return(values)

def QA_fetch_get_stock_real(code):
    quotation = easyquotation.use('sina')
    values = quotation.real(code)[code]
    return(values)

def QA_fetch_get_stock_close(code):
    return(float(QA_fetch_get_stock_realtime(code)['close']))

def QA_fetch_get_stock_realtm_ask(code):
    return(float(QA_fetch_get_stock_real(code)['ask1']))

def QA_fetch_get_stock_realtm_askvol(code):
    return(float(QA_fetch_get_stock_real(code)['aks1_volume']))

def QA_fetch_get_stock_realtm_askvol5(code):
    res = float(QA_fetch_get_stock_realtime(code)[['aks1_volume','aks2_volume','aks3_volume','aks4_volume','aks5_volume']])
    return(float(res.aks1_volume + res.aks2_volume + res.aks3_volume + res.aks4_volume + res.aks5_volume))

def QA_fetch_get_stock_realtm_bid(code):
    return(float(QA_fetch_get_stock_real(code)['bid1']))

def QA_fetch_get_stock_realtm_bidvol(code):
    return(float(QA_fetch_get_stock_real(code)['bid1_volume']))

def QA_fetch_get_stock_realtm_bidvol5(code):
    res = float(QA_fetch_get_stock_realtime(code)[['bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume']])
    return(float(res.bid1_volume + res.bid2_volume + res.bid3_volume + res.bid4_volume + res.bid5_volume))

def QA_fetch_get_usstock_adj():
    pass

def QA_fetch_get_usstock_cik():
    pass

def QA_fetch_get_usstock_financial():
    pass

def QA_fetch_get_usstock_financial_calendar():
    pass

def QA_fetch_get_stock_industryinfo(file_name='tdxhy.cfg'):
    return(pd.read_csv(tdx_dir+file_name,
                       header=None,
                       sep='|',
                       dtype=str,
                       names=['market','code','TDXHY','SWHY','HHY'],
                       encoding='gb18030'))

def QA_fetch_get_index_info(file_name='tdxzs.cfg'):
    return(pd.read_csv(tdx_dir+file_name,
                       header=None,
                       sep='|',
                       dtype=str,
                       names=['index_name','code','cate','unknown1','unknown2','HY'],
                       encoding='gb18030'))

def QA_fetch_get_stock_delist():
    sh = ak.stock_info_sh_delist(indicator="终止上市公司")[['COMPANY_CODE','SECURITY_ABBR_A','LISTING_DATE','QIANYI_DATE']]
    sz = ak.stock_info_sz_delist(indicator="终止上市公司")
    sz.columns = ['code','name','LISTING_DATE','QIANYI_DATE']
    sh.columns = ['code','name','LISTING_DATE','QIANYI_DATE']
    sh = sh.assign(sse = 'sh')
    sz = sz.assign(sse = 'sz')
    sz = sz.append(sh)
    sz = sz.assign(QIANYI_DATE = sz.QIANYI_DATE.apply(lambda x:str(x)[0:10]))
    return(sz)

