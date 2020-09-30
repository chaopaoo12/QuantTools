from QUANTTOOLS.QAStockETL.Crawly import get_usstock_list_sina
import akshare as ak

def QA_fetch_get_usstock_list_sina():
    data = get_usstock_list_sina()
    return(data)

def QA_fetch_get_usstock_list_akshare():
    data = ak.stock_us_fundamental(stock="GOOGL", symbol="info")
    data = data[['ticker','comp_name','comp_name_2','exchange','country_code','zacks_x_sector_desc','zacks_m_ind_desc','zacks_x_ind_desc']]
    data = data.rename(columns={'ticker':'code','comp_name_2':'name','zacks_x_ind_desc':'industry'})
    #data = data.assign(cik=data.ticker.apply())
    return(data)