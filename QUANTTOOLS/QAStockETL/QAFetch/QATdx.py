from QUANTAXIS import QA_fetch_get_future_day,QA_fetch_get_stock_realtime

QA_fetch_get_usstock_day = QA_fetch_get_future_day

def QA_fetch_get_stock_close(code):
    return(float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['last_close']))

def QA_fetch_get_stock_realtm_ask(code):
    return(float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['ask1']))

def QA_fetch_get_stock_realtm_askvol(code):
    return(float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['ask_vol1']))

def QA_fetch_get_stock_realtm_askvol5(code):
    aks_vol1 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['aks_vol1'])
    aks_vol2 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['aks_vol2'])
    aks_vol3 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['aks_vol3'])
    aks_vol4 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['aks_vol4'])
    aks_vol5 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['aks_vol5'])
    return(aks_vol1+aks_vol2+aks_vol3+aks_vol4+aks_vol5)

def QA_fetch_get_stock_realtm_bid(code):
    return(float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid1']))

def QA_fetch_get_stock_realtm_bidvol(code):
    return(float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol1']))

def QA_fetch_get_stock_realtm_bidvol5(code):
    bid_vol1 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol1'])
    bid_vol2 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol2'])
    bid_vol3 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol3'])
    bid_vol4 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol4'])
    bid_vol5 = float(QA_fetch_get_stock_realtime('tdx', code).reset_index('datetime')['bid_vol5'])
    return(bid_vol1+bid_vol2+bid_vol3+bid_vol4+bid_vol5)

def QA_fetch_get_usstock_adj():
    pass

def QA_fetch_get_usstock_cik():
    pass

def QA_fetch_get_usstock_financial():
    pass

def QA_fetch_get_usstock_financial_calendar():
    pass