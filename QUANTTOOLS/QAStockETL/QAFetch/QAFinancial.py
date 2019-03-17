from QUANTTOOLS.QAStockETL.Crawly import get_stock_report_ths

def QA_fetch_get_stock_report_ths(code):
    data = get_stock_report_ths(code)
    return(data)
