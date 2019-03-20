from QUANTTOOLS.QAStockETL.QAUtil import alpha

def QA_fetch_get_stock_alpha(code, date):
    data = alpha(code, date).reset_index()
    names = list(data.columns)
    names[0] = 'code'
    data.columns = names
    return(data)
