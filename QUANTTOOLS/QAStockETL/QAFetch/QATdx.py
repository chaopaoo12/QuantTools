from QUANTAXIS import QA_fetch_get_future_day, QA_fetch_stock_min_adv, QA_fetch_stock_info, QA_fetch_index_list
from QUANTTOOLS.QAStockETL.QAData.database_settings import tdx_dir
from QUANTAXIS.QAUtil import (QA_util_today_str, QA_util_get_pre_trade_date, QA_util_get_pre_trade_date,
                              QA_util_get_trade_range, QA_util_get_real_date,
                              QA_util_if_trade,QA_util_get_last_day,
                              QA_util_date_stamp)
from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_min as QA_fetch_get_stock_min_a
from akshare import stock_info_a_code_name
from pytdx.reader import BlockReader
import easyquotation
import pandas as pd
import numpy as np
import akshare as ak
import time
import multiprocessing
from functools import partial

def QA_fetch_get_usstock_day(code, start, end):
    data = QA_fetch_get_future_day('tdx',code, start, end)
    data = data.rename(columns={'trade':'vol','':'','':''})
    data = data[['open','close','high','low','vol','amount','date','code','date_stamp']]
    return(data)

def QA_fetch_get_stock_realtime(code, source ='qq'):
    try_time = 1

    while try_time <= 3:
        try:
            quotation = easyquotation.use(source)
            values = pd.DataFrame(quotation.stocks(code)).T
            values.index.name = 'code'
        except:
            values = None
        if values is None:
            try_time += 1
            time.sleep(3)
        else:
            break

    return(values)


def QA_fetch_get_stock_real(code):
    values = QA_fetch_get_stock_realtime(code).loc[code]
    return(values)

def QA_fetch_get_stock_close(code):
    return(float(QA_fetch_get_stock_realtime(code)['close']))

def QA_fetch_get_stock_realtm_ask(code):
    return(float(QA_fetch_get_stock_real(code)['ask1']))

def QA_fetch_get_stock_realtm_askvol(code):
    return(float(QA_fetch_get_stock_real(code)['ask1_volume']))

def QA_fetch_get_stock_realtm_askvol5(code):
    res = float(QA_fetch_get_stock_realtime(code)[['ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']])
    return(float(res.ask1_volume + res.ask2_volume + res.ask3_volume + res.ask4_volume + res.ask5_volume))

def QA_fetch_get_stock_realtm_bid(code):
    return(float(QA_fetch_get_stock_real(code)['bid1']))

def QA_fetch_get_stock_realtm_bidvol(code):
    return(float(QA_fetch_get_stock_real(code)['bid1_volume']))

def QA_fetch_get_stock_realtm_bidvol5(code):
    res = float(QA_fetch_get_stock_realtime(code)[['bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume']])
    return(float(res.bid1_volume + res.bid2_volume + res.bid3_volume + res.bid4_volume + res.bid5_volume))

def QA_fetch_get_usstock_adj(code):
    qfq_df = ak.stock_us_daily(symbol=code, adjust="qfq-factor").reset_index()
    qfq_df = qfq_df.assign(code = code)
    qfq_df = qfq_df.rename(columns = {'qfq_factor':'adj'})
    qfq_df = qfq_df.assign(date=pd.to_datetime(qfq_df.date))
    qfq_df[['adj','adjust']] = qfq_df[['adj','adjust']].apply(pd.to_numeric,axis=0)
    return(qfq_df)

def QA_fetch_get_usstock_pe(code):
    qfq_df = ak.stock_us_fundamental(stock=code, symbol="PE").reset_index()
    qfq_df = qfq_df.assign(code = code)
    qfq_df = qfq_df.rename(columns = {'pe_ratio':'pe'})
    qfq_df = qfq_df.assign(date_stamp=qfq_df.date.apply(lambda x:QA_util_date_stamp(str(x))),
                           date=pd.to_datetime(qfq_df.date))
    qfq_df[['stock_price','pe']] = qfq_df[['stock_price','pe']].apply(pd.to_numeric,axis=0)
    return(qfq_df)

def QA_fetch_get_usstock_pb(code):
    qfq_df = ak.stock_us_fundamental(stock=code, symbol="PB").reset_index()
    qfq_df = qfq_df.assign(code = code)
    qfq_df = qfq_df.rename(columns = {'price_to_book_ratio':'pb'})
    qfq_df = qfq_df.assign(date_stamp=qfq_df.date.apply(lambda x:QA_util_date_stamp(str(x))),
                           date=pd.to_datetime(qfq_df.date))
    qfq_df[['stock_price','pb']] = qfq_df[['stock_price','pb']].apply(pd.to_numeric,axis=0)
    return(qfq_df)

def QA_fetch_get_usstock_cik():
    pass

def QA_fetch_get_usstock_financial():
    pass

def QA_fetch_get_usstock_financial_calendar():
    pass

def QA_fetch_get_stock_industry(stock_all):
    stock_all = stock_all.drop_duplicates('code')
    stock_industry = QA_fetch_get_stock_industryinfo().fillna('0')
    index_info = QA_fetch_get_index_info()
    stock_info = QA_fetch_stock_info(stock_all.code.tolist())[['code','province','ipo_date']]

    name_dict = index_info[['code','index_name']].set_index('code').T.to_dict('records')[0]
    hy_dict = index_info[['HY','index_name']].set_index('HY').T.to_dict('records')[0]
    name_dict['880200'] = 'unknown'
    name_dict['0'] = 'unknown'
    hy_dict['T00'] = 'unknown'
    hy_dict['0'] = 'unknown'
    hy_dict['100000'] = 'unknown'

    stock_info = stock_info.assign(AREA= stock_info.province.apply(lambda x:'8802'+str(x) if len(str(x)) ==2 else '88020'+str(x)).apply(lambda x:name_dict[x]),
                                   ipo_date= stock_info.ipo_date.apply(lambda x:str(x)))
    stock_industry = stock_industry.assign(TDX=stock_industry.TDXHY.apply(lambda x:hy_dict[x]),
                                           SWHY=stock_industry.SWHY.apply(lambda x:str(x[0:4]+'00') if x is not None and x is not np.nan and len(x) >= 4 else '0').apply(lambda x:hy_dict[x]),
                                           NAME=stock_industry.code.apply(lambda x:stock_all[stock_all.code==x].name.reset_index(drop=True)),
                                           AREA=stock_industry.code.apply(lambda x:stock_info[stock_info.code==x].AREA.reset_index(drop=True)),
                                           IPO=stock_industry.code.apply(lambda x:stock_info[stock_info.code==x].ipo_date.reset_index(drop=True)))
    return(stock_industry)

def QA_fetch_get_stock_industryinfo(file_name='tdxhy.cfg'):
    return(pd.read_csv(tdx_dir+file_name,
                       header=None,
                       sep='|',
                       dtype=str,
                       names=['market','code','TDXHY','SWHY','HHY','XHY'],
                       encoding='gb18030'))

def QA_fetch_get_index_code(file_name=['block_fg.dat','block_gn.dat','block_zs.dat']):
    res = pd.DataFrame()
    if len(file_name) > 1:
        for i in file_name:
            data = BlockReader().get_df(tdx_dir+i)
            res = res.append(data)
        return(res)
    else:
        return(BlockReader().get_df(tdx_dir+file_name[0]))


def QA_fetch_get_index_info(file_name=['tdxzs.cfg','tdxzs2.cfg','tdxzs3.cfg']):
    res = pd.DataFrame()
    if len(file_name) > 1:
        for i in file_name:
            data = pd.read_csv(tdx_dir+i,
                               header=None,
                               sep='|',
                               dtype=str,
                               names=['index_name','code','cate','unknown1','unknown2','HY'],
                               encoding='gb18030')
            res = res.append(data)
        return(res)
    else:
        return(pd.read_csv(tdx_dir+file_name[0],
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

def QA_fetch_get_stock_half_realtime(code, date = QA_util_today_str(), source = 'sina'):
    try:
        quotation = easyquotation.use(source)
        res = pd.DataFrame(quotation.stocks(code)).T[['date','open','high','low','now','turnover','volume','close']]
        res = res.reset_index().rename(columns={'index':'code',
                                                'close':'prev_close',
                                                'now':'close',
                                                'turnover':'volume',
                                                'volume':'amount'})
        res['date'] = pd.to_datetime(res['date'])
        res[['open','high','low','close','volume','amount','prev_close']] = res[['open','high','low','close','volume','amount','prev_close']].apply(pd.to_numeric)
        res['avg_price'] = res['amount']/res['volume']
        res = res[res.volume > 0]
    except:
        source = 'qq'
        quotation = easyquotation.use(source)
        res = pd.DataFrame(quotation.stocks(code)).T[['datetime','open','high','low','now','volume','成交额(万)','close']]
        res = res.reset_index().rename(columns={'index':'code',
                                                'close':'prev_close',
                                                'now':'close',
                                                'turnover':'volume',
                                                '成交额(万)':'amount'})
        res['date'] = pd.to_datetime(res['datetime'])
        res[['open','high','low','close','volume','amount','prev_close']] = res[['open','high','low','close','volume','amount','prev_close']].apply(pd.to_numeric)
        res['avg_price'] = res['amount']/res['volume']
        res = res[res.volume > 0]
    res = res[res.date == date]
    res['date_stamp'] = res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
    return(res)

def QA_fetch_get_stock_half_real(code, date, source = 'sina'):
    res = QA_fetch_get_stock_realtime(code, source = source)
    res = res.reset_index().rename(columns={'index':'code',
                                            'close':'prev_close',
                                            'now':'close',
                                            'turnover':'volume',
                                            'volume':'amount'})
    res['date'] = pd.to_datetime(res['date'])
    res[['open','high','low','close','volume','amount','prev_close']] = res[['open','high','low','close','volume','amount','prev_close']].apply(pd.to_numeric)
    res['avg_price'] = res['amount']/res['volume']
    res['date_stamp'] = res['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
    res = res.loc[res.name.apply(lambda x:str(x).startswith('N') == 0)][['date','code','open','high','low','close','volume','amount','prev_close','avg_price','date_stamp']]
    res = res[res.volume > 0]
    res = res[res.date == date]
    return(res)

def QA_fetch_get_stockcode_real(code, date = QA_util_today_str(), source = 'qq'):
    res = QA_fetch_get_stock_half_realtime(code=code, date = date, source = source)

    return(res.code.unique().tolist())

def half_ohlc(data):
    data = data.reset_index().set_index('datetime')
    res = data.resample('12H').agg({'open': 'first', 'high': 'max',  'low': 'min', 'close': 'last','volume': 'sum','amount': 'sum'})
    return(res)

def QA_fetch_get_stock_half(code, start, end):
    if QA_util_if_trade(start):
        start_date = QA_util_get_last_day(start)
    else:
        start_date = QA_util_get_real_date(start)

    data = QA_fetch_stock_min_adv(code, start_date, end, frequence='60min')
    pctchange = data.to_qfq().data.groupby('code').apply(half_ohlc)
    pctchange = pctchange[pctchange.volume > 0]
    pctchange = pctchange.assign(pctchange = pctchange.close/pctchange.close.shift()-1)
    data = data.data.groupby('code').apply(half_ohlc)
    data = data[data.volume > 0]
    price = data.join(pctchange['pctchange']).reset_index().set_index('datetime')
    price = price.between_time("00:00", "09:00").reset_index().rename(columns={'datetime':'date'})
    price['date_stamp'] = price['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))
    return(price)

def fetch_get_stock_code_all():
    return(stock_info_a_code_name())

def QA_fetch_get_stock_tfp(date):
    stock_tfp_em_df = ak.stock_tfp_em(date=date.replace('-',''))
    tfp = stock_tfp_em_df[(stock_tfp_em_df['停牌时间'].apply(lambda x:str(x)) <= date)&
                          (stock_tfp_em_df['停牌截止时间'].apply(lambda x:str(x)) >= date)]['代码'].tolist()
    return(tfp)

def QA_fetch_get_stock_min_tdx(code, start, end, frequence):

    if isinstance(code,list):
        pool = multiprocessing.Pool(20)
        with pool as p:
            res = p.map(partial(QA_fetch_get_stock_min_a, start=start, end=end, frequence=frequence), code)
        data = pd.concat(res, axis=0)
    elif isinstance(code, str):
        data = QA_fetch_get_stock_min_a(code=code, start=start, end=end, frequence=frequence)

    else:
        data = None
    try:
        data = data.assign(volume=data.vol)
    except:
        data = None

    return(data)


if __name__ == '__main__':
    pass