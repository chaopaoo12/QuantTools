

from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_block_adv,QA_fetch_index_list_adv,
                                               QA_fetch_stock_day_adv)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_usstock_xq_day_adv,QA_fetch_usstock_list
from QUANTAXIS.QAUtil import (QA_util_today_str,QA_util_log_info)
from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_financial_report_adv,QA_fetch_stock_financial_calendar_adv,
                                           QA_fetch_stock_divyield_adv,QA_fetch_stock_shares_adv,
                                           QA_fetch_financial_report_wy_adv,

                                           QA_fetch_get_stock_etlday, QA_fetch_get_usstock_etlday,
                                           QA_fetch_get_stock_etlhalf,

                                           QA_fetch_stock_alpha_adv,QA_fetch_stock_alpha101_adv,
                                           QA_fetch_stock_alpha191half_adv,QA_fetch_stock_alpha101half_adv,
                                           QA_fetch_stock_technical_index_adv,
                                           QA_fetch_stock_fianacial_adv,QA_fetch_stock_financial_percent_adv,

                                           QA_fetch_index_info,
                                           QA_fetch_index_alpha_adv,QA_fetch_index_alpha101_adv,
                                           QA_fetch_index_technical_index_adv,

                                           QA_fetch_usstock_alpha_adv,QA_fetch_usstock_alpha101_adv,
                                           QA_fetch_usstock_technical_index_adv,
                                           QA_fetch_usstock_financial_percent_adv
                                           )
from QUANTAXIS.QAFetch.QAQuery import ( QA_fetch_stock_basic_info_tushare, QA_fetch_stock_xdxr)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_sql_store_mysql
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_process_financial)
import pandas as pd
import datetime

def QA_fetch_index_cate(data, stock_code):
    try:
        return data.loc[stock_code]['cate']
    except:
        return None

def QA_etl_stock_list(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK LIST ==== {}'.format(str(datetime.date.today())), ui_log)
    QA_util_sql_store_mysql(QA_fetch_stock_list_adv().reset_index(drop=True), "stock_list",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK LIST HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_shares(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK SHARES ==== {}'.format(str(datetime.date.today())), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_shares_adv(codes).data
    QA_util_sql_store_mysql(data, "stock_shares",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK SHARES HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_info(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK INFO ==== {}'.format(str(datetime.date.today())), ui_log)
    data = pd.DataFrame(QA_fetch_stock_basic_info_tushare())
    data = data.drop("_id", axis=1)
    QA_util_sql_store_mysql(data, "stock_info",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK INFO HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_xdxr(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info('##JOB Now ETL STOCK XDXR ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == "all":
        data = QA_fetch_stock_xdxr(codes).reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='replace')
    elif type == "day":
        data = QA_fetch_stock_xdxr(codes, mark_day)
        if data is None:
            QA_util_log_info("We have no XDXR data for the day {}".format(mark_day))
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_xdxr",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK XDXR HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_usstock_day(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL USSTOCK DAY ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    if type == "all":
        for i in codes:
            QA_util_log_info('The {} of Total {}====={}'.format
                             ((codes.index(i) +1), len(codes), i))
            data = QA_fetch_get_usstock_etlday(i)
            if data is None:
                QA_util_log_info("We have no MARKET data for the ======= {}".format(i))
            else:
                QA_util_sql_store_mysql(data, "usstock_market_day",if_exists='append')
                QA_util_log_info(
                    '##JOB ETL USSTOCK DAY HAS BEEN SAVED ==== {}'.format(i), ui_log)
    elif type == "day":
        data = QA_fetch_get_usstock_etlday(codes, mark_day, mark_day)
        if data is None:
            QA_util_log_info("We have no MARKET data for the day {}".format(mark_day))
        else:
            QA_util_sql_store_mysql(data, "usstock_market_day",if_exists='append')
            QA_util_log_info(
                '##JOB ETL USSTOCK DAY HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_stock_day(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK DAY ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == "all":
        for i in codes:
            QA_util_log_info('The {} of Total {}====={}'.format
                             ((codes.index(i) +1), len(codes), i))
            data = QA_fetch_get_stock_etlday(i)
            if data is None:
                QA_util_log_info("We have no MARKET data for the ======= {}".format(i))
            else:
                QA_util_sql_store_mysql(data, "stock_market_day",if_exists='append')
                QA_util_log_info(
                    '##JOB ETL STOCK DAY HAS BEEN SAVED ==== {}'.format(i), ui_log)
    elif type == "day":
        for i in codes:
            QA_util_log_info('The {} of Total {}====={}'.format
                             ((codes.index(i) +1), len(codes), i))
            data = QA_fetch_get_stock_etlday(codes, mark_day, mark_day)
            if data is None:
                QA_util_log_info("We have no MARKET data for the day {}".format(mark_day))
            else:
                QA_util_sql_store_mysql(data, "stock_market_day",if_exists='append')
                QA_util_log_info(
                    '##JOB ETL STOCK DAY HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_stock_half(type = "day", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK HALF ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == "all":
        for i in codes:
            QA_util_log_info('The {} of Total {}====={}'.format
                             ((codes.index(i) +1), len(codes), i))
            data = QA_fetch_get_stock_etlhalf(i)
            if data is None:
                QA_util_log_info("We have no MARKET data for the ======= {}".format(i))
            else:
                QA_util_sql_store_mysql(data, "stock_market_half",if_exists='append')
                QA_util_log_info(
                    '##JOB ETL STOCK HALF HAS BEEN SAVED ==== {}'.format(i), ui_log)
    elif type == "day":
        for i in codes:
            QA_util_log_info('The {} of Total {}====={}'.format
                             ((codes.index(i) +1), len(codes), i))
            data = QA_fetch_get_stock_etlhalf(i, mark_day, mark_day)
            if data is None:
                QA_util_log_info("We have no MARKET data for the code {}".format(i))
            else:
                QA_util_sql_store_mysql(data, "stock_market_half",if_exists='append')
                QA_util_log_info(
                    '##JOB ETL STOCK HALF HAS BEEN SAVED ==== {}'.format(i), ui_log)

def QA_etl_stock_financial(type = "crawl", start_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK FINANCIAL REPORT ==== {}'.format(start_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == 'all':
        data = QA_fetch_financial_report_adv(codes).data
        columns = [i for i in list(data.columns) if i.startswith('unknown') == False and i.isdigit() == False and i.startswith('IS_R') == False]
        QA_util_sql_store_mysql(data[columns].reset_index(drop=True).fillna(0), "stock_financial",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_adv(codes,start_date,type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK FINANCIAL REPORT HAS BEEN SAVED ==== {}'.format(start_date), ui_log)
        else:
            data = data.reset_index(drop=True).drop("_id",1).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK FINANCIAL REPORT HAS BEEN SAVED ==== {}'.format(start_date), ui_log)

def QA_etl_stock_calendar(type = "crawl", start = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK CALENDAR ==== {}'.format(start), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == "all":
        data = QA_fetch_stock_financial_calendar_adv(codes,start = "all", type = 'report').data.reset_index(drop=True)
        QA_util_sql_store_mysql(data, "stock_calendar",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_financial_calendar_adv(codes, start, type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK CALENDAR HAS BEEN SAVED ==== {}'.format(start), ui_log)
        else:
            data = data.reset_index(drop=True)
            QA_util_sql_store_mysql(data, "stock_calendar",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK CALENDAR HAS BEEN SAVED ==== {}'.format(start), ui_log)

def QA_etl_stock_block(ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK Block ==== {}'.format(str(datetime.date.today())), ui_log)
    data = QA_fetch_stock_block_adv().data.reset_index()
    QA_util_sql_store_mysql(data, "stock_block",if_exists='replace')
    QA_util_log_info(
        '##JOB ETL STOCK Block HAS BEEN SAVED ==== {}'.format(str(datetime.date.today())), ui_log)

def QA_etl_stock_divyield(type = "crawl", mark_day = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK divyield ==== {}'.format(mark_day), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == "all":
        data = QA_fetch_stock_divyield_adv(codes,start = "all").data.reset_index()
        QA_util_sql_store_mysql(data, "stock_divyield",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_stock_divyield_adv(codes, mark_day).data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK Divyield HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)
        else:
            data = data.reset_index()
            QA_util_sql_store_mysql(data, "stock_divyield",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK Divyield HAS BEEN SAVED ==== {}'.format(mark_day), ui_log)

def QA_etl_process_financial_day(type = "day", deal_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL PROCESS FINANCIAL ==== {}'.format(deal_date), ui_log)
    if type == "day":
        #print("Step One =================")
        QA_util_process_financial(deal_date=deal_date)
        QA_util_log_info(
            '##JOB Now ETL PROCESS FINANCIAL HAS BEEN SAVED ==== {}'.format(deal_date), ui_log)
    elif type == "all":
        QA_util_log_info("Run This JOB in DataBase")

def QA_etl_stock_financial_wy(type = "crawl", start_date = str(datetime.date.today()),ui_log= None):
    QA_util_log_info(
        '##JOB Now ETL STOCK FINANCIAL REPORT WY ==== {}'.format(start_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    if type == 'all':
        data = QA_fetch_financial_report_wy_adv(codes).data.reset_index(drop=True).fillna(0)
        QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='replace')
    elif type == "crawl":
        data = QA_fetch_financial_report_wy_adv(codes,start_date,type = 'crawl').data
        if data is None:
            QA_util_log_info(
                '##JOB NO STOCK FINANCIAL REPORT WY HAS BEEN SAVED ==== {}'.format(start_date), ui_log)
        else:
            data = data.reset_index(drop=True).fillna(0)
            QA_util_sql_store_mysql(data, "stock_financial_wy",if_exists='append')
            QA_util_log_info(
                '##JOB ETL STOCK FINANCIAL REPORT WY HAS BEEN SAVED ==== {}'.format(start_date), ui_log)

def QA_etl_stock_alpha_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK ALPHA191 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_alpha_adv(codes, start_date, end_date).data[['alpha_001','alpha_002','alpha_003','alpha_004','alpha_005','alpha_006',
                                                                       'alpha_007','alpha_008','alpha_009','alpha_010','alpha_011','alpha_012',
                                                                       'alpha_013','alpha_014','alpha_015','alpha_016','alpha_018','alpha_019',
                                                                       'alpha_020','alpha_021','alpha_022','alpha_023','alpha_024','alpha_025',
                                                                       'alpha_026','alpha_028','alpha_029','alpha_031','alpha_032','alpha_033',
                                                                       'alpha_034','alpha_035','alpha_036','alpha_037','alpha_038','alpha_039',
                                                                       'alpha_040','alpha_041','alpha_042','alpha_043','alpha_044','alpha_045',
                                                                       'alpha_046','alpha_047','alpha_048','alpha_049','alpha_052','alpha_053',
                                                                       'alpha_054','alpha_056','alpha_057','alpha_058','alpha_059','alpha_060',
                                                                       'alpha_061','alpha_062','alpha_063','alpha_064','alpha_065','alpha_066',
                                                                       'alpha_067','alpha_068','alpha_070','alpha_071','alpha_072','alpha_074',
                                                                       'alpha_076','alpha_077','alpha_078','alpha_079','alpha_080','alpha_081',
                                                                       'alpha_082','alpha_083','alpha_084','alpha_085','alpha_086','alpha_087',
                                                                       'alpha_088','alpha_089','alpha_090','alpha_091','alpha_092','alpha_093',
                                                                       'alpha_094','alpha_095','alpha_096','alpha_097','alpha_098','alpha_099',
                                                                       'alpha_100','alpha_101','alpha_102','alpha_103','alpha_104','alpha_105',
                                                                       'alpha_106','alpha_107','alpha_108','alpha_109','alpha_111','alpha_112',
                                                                       'alpha_113','alpha_114','alpha_115','alpha_116','alpha_117','alpha_118',
                                                                       'alpha_119','alpha_120','alpha_122','alpha_123','alpha_124','alpha_125',
                                                                       'alpha_126','alpha_127','alpha_128','alpha_129','alpha_130','alpha_132',
                                                                       'alpha_133','alpha_134','alpha_135','alpha_136','alpha_137','alpha_138',
                                                                       'alpha_139','alpha_141','alpha_142','alpha_145','alpha_148','alpha_150',
                                                                       'alpha_152','alpha_153','alpha_154','alpha_155','alpha_156','alpha_157',
                                                                       'alpha_158','alpha_159','alpha_160','alpha_161','alpha_162','alpha_163',
                                                                       'alpha_164','alpha_167','alpha_168','alpha_169','alpha_170','alpha_171',
                                                                       'alpha_172','alpha_173','alpha_174','alpha_175','alpha_176','alpha_177',
                                                                       'alpha_178','alpha_179','alpha_180','alpha_184','alpha_185','alpha_186',
                                                                       'alpha_187','alpha_188','alpha_189','alpha_191'
                                                                       ]]
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_alpha191",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_alpha191half_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK ALPHA191 HALF ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_alpha191half_adv(codes, start_date, end_date).data[['alpha_001','alpha_002','alpha_003','alpha_004','alpha_005','alpha_006',
                                                                              'alpha_007','alpha_008','alpha_009','alpha_010','alpha_011','alpha_012',
                                                                              'alpha_013','alpha_014','alpha_015','alpha_016','alpha_018','alpha_019',
                                                                              'alpha_020','alpha_021','alpha_022','alpha_023','alpha_024','alpha_025',
                                                                              'alpha_026','alpha_028','alpha_029','alpha_031','alpha_032','alpha_033',
                                                                              'alpha_034','alpha_035','alpha_036','alpha_037','alpha_038','alpha_039',
                                                                              'alpha_040','alpha_041','alpha_042','alpha_043','alpha_044','alpha_045',
                                                                              'alpha_046','alpha_047','alpha_048','alpha_049','alpha_052','alpha_053',
                                                                              'alpha_054','alpha_056','alpha_057','alpha_058','alpha_059','alpha_060',
                                                                              'alpha_061','alpha_062','alpha_063','alpha_064','alpha_065','alpha_066',
                                                                              'alpha_067','alpha_068','alpha_070','alpha_071','alpha_072','alpha_074',
                                                                              'alpha_076','alpha_077','alpha_078','alpha_079','alpha_080','alpha_081',
                                                                              'alpha_082','alpha_083','alpha_084','alpha_085','alpha_086','alpha_087',
                                                                              'alpha_088','alpha_089','alpha_090','alpha_091','alpha_092','alpha_093',
                                                                              'alpha_094','alpha_095','alpha_096','alpha_097','alpha_098','alpha_099',
                                                                              'alpha_100','alpha_101','alpha_102','alpha_103','alpha_104','alpha_105',
                                                                              'alpha_106','alpha_107','alpha_108','alpha_109','alpha_111','alpha_112',
                                                                              'alpha_113','alpha_114','alpha_115','alpha_116','alpha_117','alpha_118',
                                                                              'alpha_119','alpha_120','alpha_122','alpha_123','alpha_124','alpha_125',
                                                                              'alpha_126','alpha_127','alpha_128','alpha_129','alpha_130','alpha_132',
                                                                              'alpha_133','alpha_134','alpha_135','alpha_136','alpha_137','alpha_138',
                                                                              'alpha_139','alpha_141','alpha_142','alpha_145','alpha_148','alpha_150',
                                                                              'alpha_152','alpha_153','alpha_154','alpha_155','alpha_156','alpha_157',
                                                                              'alpha_158','alpha_159','alpha_160','alpha_161','alpha_162','alpha_163',
                                                                              'alpha_164','alpha_167','alpha_168','alpha_169','alpha_170','alpha_171',
                                                                              'alpha_172','alpha_173','alpha_174','alpha_175','alpha_176','alpha_177',
                                                                              'alpha_178','alpha_179','alpha_180','alpha_184','alpha_185','alpha_186',
                                                                              'alpha_187','alpha_188','alpha_189','alpha_191'
                                                                              ]]
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK ALPHA191 HALF HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_alpha191_half",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK ALPHA191 HALF HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_alpha101_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK ALPHA101 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_alpha101_adv(codes, start_date, end_date).data[['alpha001','alpha002','alpha003','alpha004','alpha005','alpha006',
                                                                          'alpha007','alpha008','alpha009','alpha010','alpha011','alpha012',
                                                                          'alpha013','alpha014','alpha015','alpha016','alpha017','alpha018',
                                                                          'alpha019','alpha020','alpha021','alpha022','alpha023','alpha024',
                                                                          'alpha025','alpha026','alpha027','alpha028','alpha029','alpha030',
                                                                          'alpha031','alpha032','alpha033','alpha034','alpha035','alpha036',
                                                                          'alpha037','alpha038','alpha039','alpha040','alpha041','alpha042',
                                                                          'alpha043','alpha044','alpha045','alpha046','alpha047','alpha049',
                                                                          'alpha050','alpha051','alpha052','alpha053','alpha054','alpha055',
                                                                          'alpha057','alpha060','alpha061','alpha062','alpha064','alpha065',
                                                                          'alpha066','alpha068','alpha071','alpha072','alpha073','alpha074',
                                                                          'alpha075','alpha077','alpha078','alpha081','alpha083','alpha085',
                                                                          'alpha086','alpha088','alpha092','alpha094','alpha095','alpha096',
                                                                          'alpha098','alpha099','alpha101']]
    if data is None:
        QA_util_log_info('##JOB NO STOCK ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_alpha101",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_alpha101half_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK ALPHA101 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_alpha101half_adv(codes, start_date, end_date).data[['alpha001','alpha002','alpha003','alpha004','alpha005','alpha006',
                                                                              'alpha007','alpha008','alpha009','alpha010','alpha011','alpha012',
                                                                              'alpha013','alpha014','alpha015','alpha016','alpha017','alpha018',
                                                                              'alpha019','alpha020','alpha021','alpha022','alpha023','alpha024',
                                                                              'alpha025','alpha026','alpha027','alpha028','alpha029','alpha030',
                                                                              'alpha031','alpha032','alpha033','alpha034','alpha035','alpha036',
                                                                              'alpha037','alpha038','alpha039','alpha040','alpha041','alpha042',
                                                                              'alpha043','alpha044','alpha045','alpha046','alpha047','alpha049',
                                                                              'alpha050','alpha051','alpha052','alpha053','alpha054','alpha055',
                                                                              'alpha057','alpha060','alpha061','alpha062','alpha064','alpha065',
                                                                              'alpha066','alpha068','alpha071','alpha072','alpha073','alpha074',
                                                                              'alpha075','alpha077','alpha078','alpha081','alpha083','alpha085',
                                                                              'alpha086','alpha088','alpha092','alpha094','alpha095','alpha096',
                                                                              'alpha098','alpha099','alpha101']]
    if data is None:
        QA_util_log_info('##JOB NO STOCK ALPHA101 HALF HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_alpha101_half",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK ALPHA101 HALF HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_technical_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK TECHNICAL ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_technical_index_adv(codes, start_date, end_date).data
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_technical",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_technical_week(start_date = QA_util_today_str(), end_date= None,ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK TECHNICAL WEEK ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_technical_index_adv(codes, start_date, end_date,type='week').data
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_technical_week",if_exists='append')
        QA_util_log_info('##JOB ETL STOCK TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_financial_day(start_date = QA_util_today_str(), end_date= None,ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL STOCK QUANT FINANCIAL ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_fianacial_adv(codes, start_date, end_date).data[[ 'INDUSTRY','TOTAL_MARKET', 'TRA_RATE', 'DAYS',
                                                                            'AVG5','AVG10','AVG20','AVG30','AVG60',
                                                                            'LAG','LAG5','LAG10','LAG20','LAG30','LAG60',
                                                                            'AVG5_TOR', 'AVG20_TOR','AVG30_TOR','AVG60_TOR',
                                                                            'GROSSMARGIN','NETPROFIT_INRATE','OPERATINGRINRATE','NETCASHOPERATINRATE',
                                                                            'PB', 'PBG', 'PC', 'PE_TTM', 'PEEGL_TTM', 'PEG', 'PM', 'PS','PSG','PT',
                                                                            #'I_PB','I_PE','I_PEEGL','I_ROE','I_ROE_TOTAL','I_ROA','I_ROA_TOTAL','I_GROSSMARGIN',
                                                                            'PE_RATE','PEEGL_RATE','PB_RATE','ROE_RATE','ROE_RATET','ROA_RATE','ROA_RATET',
                                                                            'GROSS_RATE',#'ROA_AVG5','ROE_AVG5','GROSS_AVG5','ROE_MIN','ROA_MIN','GROSS_MIN',
                                                                            #'ROE_CH','ROA_CH','GROSS_CH','OPINRATE_AVG3','NETPINRATE_AVG3',
                                                                            'RNG','RNG_L','RNG_5','RNG_10','RNG_20', 'RNG_30', 'RNG_60','RNG_90',
                                                                            'AVG5_RNG','AVG10_RNG','AVG20_RNG','AVG30_RNG','AVG60_RNG',
                                                                            'ROA', #'ROA_L2Y', 'ROA_L3Y', 'ROA_L4Y', 'ROA_LY',
                                                                            'ROE', #'ROE_L2Y', 'ROE_L3Y', 'ROE_L4Y', 'ROE_LY',
                                                                            'AVG5_CR', 'AVG10_CR','AVG20_CR','AVG30_CR','AVG60_CR',
                                                                            'AVG5_TR','AVG10_TR','AVG20_TR','AVG30_TR','AVG60_TR','TOTALPROFITINRATE',
                                                                            'AMT_L','AMT_5','AMT_10','AMT_20','AMT_30','AMT_60','AMT_90',
                                                                            'MAMT_5','MAMT_10','MAMT_20','MAMT_30','MAMT_60','MAMT_90',
                                                                            'NEG5_RT','NEG5_RATE','NEG10_RT','NEG10_RATE','NEG20_RT','NEG20_RATE',
                                                                            'NEG30_RT','NEG30_RATE','NEG60_RT','NEG60_RATE','NEG90_RT','NEG90_RATE']]
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK QUANT FINANCIAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_quant_financial",if_exists='append')
        QA_util_log_info(
            '##JOB ETL STOCK QUANT FINANCIAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_stock_financial_percent_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info(
        '##JOB Now ETL STOCK FINANCIAL PERCENT ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_stock_all()['code'])
    data = QA_fetch_stock_financial_percent_adv(codes, start_date, end_date).data
    if data is None:
        QA_util_log_info(
            '##JOB NO STOCK FINANCIAL PERCENT HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "stock_quant_financial_percent",if_exists='append')
        QA_util_log_info(
            '##JOB ETL STOCK FINANCIAL PERCENT HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_index_alpha_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info(
        '##JOB Now ETL INDEX ALPHA191 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = QA_fetch_index_info(list(QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)
    codes.extend(['000001','399001','399006'])
    data = QA_fetch_index_alpha_adv(codes, start_date, end_date).data[["alpha_001","alpha_002","alpha_003","alpha_004","alpha_005","alpha_006","alpha_007","alpha_008",
                                                                       "alpha_009","alpha_010","alpha_011","alpha_012","alpha_013","alpha_014","alpha_015","alpha_016",
                                                                       "alpha_018","alpha_019","alpha_020","alpha_021","alpha_022","alpha_023","alpha_024",
                                                                       "alpha_025","alpha_026","alpha_028","alpha_029","alpha_031","alpha_032","alpha_033","alpha_034",
                                                                       "alpha_035","alpha_036","alpha_037","alpha_038","alpha_039","alpha_040","alpha_041","alpha_042",
                                                                       "alpha_043","alpha_044","alpha_045","alpha_046","alpha_047","alpha_048","alpha_049","alpha_052",
                                                                       "alpha_053","alpha_054","alpha_056","alpha_057","alpha_058","alpha_059","alpha_060",
                                                                       "alpha_061","alpha_062","alpha_063","alpha_064","alpha_065","alpha_066","alpha_067","alpha_068",
                                                                       "alpha_070","alpha_071","alpha_072","alpha_074","alpha_076","alpha_077","alpha_078","alpha_079",
                                                                       "alpha_080","alpha_081","alpha_082","alpha_083","alpha_084","alpha_085","alpha_086","alpha_087",
                                                                       "alpha_088","alpha_089","alpha_090","alpha_091","alpha_093","alpha_094","alpha_095",
                                                                       "alpha_096","alpha_097","alpha_098","alpha_099","alpha_100","alpha_101","alpha_102","alpha_103",
                                                                       "alpha_104","alpha_105","alpha_106","alpha_107","alpha_108","alpha_109","alpha_111",
                                                                       "alpha_112","alpha_114","alpha_115","alpha_117","alpha_118","alpha_119",
                                                                       "alpha_120","alpha_122","alpha_123","alpha_124","alpha_125","alpha_126",
                                                                       "alpha_129","alpha_130","alpha_132","alpha_133","alpha_134","alpha_135","alpha_136",
                                                                       "alpha_139","alpha_141","alpha_142","alpha_145",
                                                                       "alpha_148","alpha_150","alpha_152","alpha_153","alpha_154","alpha_155","alpha_156",
                                                                       "alpha_157","alpha_158","alpha_159","alpha_160","alpha_161","alpha_162","alpha_163","alpha_164",
                                                                       "alpha_167","alpha_168","alpha_169","alpha_170","alpha_172","alpha_173","alpha_174",
                                                                       "alpha_175","alpha_176","alpha_177","alpha_178","alpha_179","alpha_180","alpha_184","alpha_185",
                                                                       "alpha_186","alpha_187","alpha_188","alpha_189","alpha_191"]]
    if data is None:
        QA_util_log_info(
            '##JOB NO INDEX ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "index_alpha191",if_exists='append')
        QA_util_log_info(
            '##JOB ETL INDEX ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_index_alpha101_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info(
        '##JOB Now ETL INDEX ALPHA101 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = QA_fetch_index_info(list(QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)
    codes.extend(['000001','399001','399006'])
    data = QA_fetch_index_alpha101_adv(codes, start_date, end_date).data[['alpha001','alpha002','alpha003','alpha004','alpha005','alpha006',
                                                                'alpha007','alpha008','alpha009','alpha010','alpha011','alpha012',
                                                                'alpha013','alpha014','alpha015','alpha016','alpha017','alpha018',
                                                                'alpha019','alpha020','alpha021','alpha022','alpha023','alpha024',
                                                                'alpha025','alpha026','alpha027','alpha028','alpha029','alpha030',
                                                                'alpha031','alpha032','alpha033','alpha034','alpha035','alpha036',
                                                                'alpha037','alpha038','alpha039','alpha040','alpha041','alpha042',
                                                                'alpha043','alpha044','alpha045','alpha046','alpha047','alpha049',
                                                                'alpha050','alpha051','alpha052','alpha053','alpha054','alpha055',
                                                                'alpha057','alpha060','alpha061','alpha062','alpha064','alpha065',
                                                                'alpha066','alpha068','alpha071','alpha072','alpha073','alpha074',
                                                                'alpha075','alpha077','alpha078','alpha081','alpha083','alpha085',
                                                                'alpha086','alpha088','alpha092','alpha094','alpha095','alpha096',
                                                                'alpha098','alpha099','alpha101']]
    if data is None:
        QA_util_log_info(
            '##JOB NO INDEX ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "index_alpha101",if_exists='append')
        QA_util_log_info(
            '##JOB ETL INDEX ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_index_technical_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL INDEX TECHNICAL ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = QA_fetch_index_info(list(QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)
    codes.extend(['000001','399001','399006'])
    data = QA_fetch_index_technical_index_adv(codes, start_date, end_date).data
    if data is None:
        QA_util_log_info(
            '##JOB NO INDEX TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "index_technical",if_exists='append')
        QA_util_log_info('##JOB ETL INDEX TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_index_technical_week(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL INDEX TECHNICAL WEEK ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = QA_fetch_index_info(list(QA_fetch_index_list_adv().code))
    codes = list(codes[codes.cate != '5'].code)
    codes.extend(['000001','399001','399006'])
    data = QA_fetch_index_technical_index_adv(codes, start_date, end_date, type='week').data
    if data is None:
        QA_util_log_info(
            '##JOB NO INDEX TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "index_technical_week",if_exists='append')
        QA_util_log_info('##JOB ETL INDEX TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_usstock_alpha_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL USSTOCK ALPHA191 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    data = QA_fetch_usstock_alpha_adv(codes, start_date, end_date).data[['alpha_001','alpha_002','alpha_003','alpha_004','alpha_005','alpha_006',
                                                                         'alpha_007','alpha_008','alpha_009','alpha_010','alpha_011','alpha_012',
                                                                         'alpha_013','alpha_014','alpha_015','alpha_016','alpha_018','alpha_019',
                                                                         'alpha_020','alpha_021','alpha_022','alpha_023','alpha_024','alpha_025',
                                                                         'alpha_026','alpha_028','alpha_029','alpha_031','alpha_032','alpha_033',
                                                                         'alpha_034','alpha_035','alpha_036','alpha_037','alpha_038','alpha_039',
                                                                         'alpha_040','alpha_041','alpha_042','alpha_043','alpha_044','alpha_045',
                                                                         'alpha_046','alpha_047','alpha_048','alpha_049','alpha_052','alpha_053',
                                                                         'alpha_054','alpha_056','alpha_057','alpha_058','alpha_059','alpha_060',
                                                                         'alpha_061','alpha_062','alpha_063','alpha_064','alpha_065','alpha_066',
                                                                         'alpha_067','alpha_068','alpha_070','alpha_071','alpha_072','alpha_074',
                                                                         'alpha_076','alpha_077','alpha_078','alpha_079','alpha_080','alpha_081',
                                                                         'alpha_082','alpha_083','alpha_084','alpha_085','alpha_086','alpha_087',
                                                                         'alpha_088','alpha_089','alpha_090','alpha_091','alpha_092','alpha_093',
                                                                         'alpha_094','alpha_095','alpha_096','alpha_097','alpha_098','alpha_099',
                                                                         'alpha_100','alpha_101','alpha_102','alpha_103','alpha_104','alpha_105',
                                                                         'alpha_106','alpha_107','alpha_108','alpha_109','alpha_111','alpha_112',
                                                                         'alpha_113','alpha_114','alpha_115','alpha_116','alpha_117','alpha_118',
                                                                         'alpha_119','alpha_120','alpha_122','alpha_123','alpha_124','alpha_125',
                                                                         'alpha_126','alpha_127','alpha_128','alpha_129','alpha_130','alpha_132',
                                                                         'alpha_133','alpha_134','alpha_135','alpha_136','alpha_137','alpha_138',
                                                                         'alpha_139','alpha_141','alpha_142','alpha_145','alpha_148','alpha_150',
                                                                         'alpha_152','alpha_153','alpha_154','alpha_155','alpha_156','alpha_157',
                                                                         'alpha_158','alpha_159','alpha_160','alpha_161','alpha_162','alpha_163',
                                                                         'alpha_164','alpha_167','alpha_168','alpha_169','alpha_170','alpha_171',
                                                                         'alpha_172','alpha_173','alpha_174','alpha_175','alpha_176','alpha_177',
                                                                         'alpha_178','alpha_179','alpha_180','alpha_184','alpha_185','alpha_186',
                                                                         'alpha_187','alpha_188','alpha_189','alpha_191'
                                                                         ]]
    if data is None:
        QA_util_log_info(
            '##JOB NO USSTOCK ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "usstock_alpha191",if_exists='append')
        QA_util_log_info('##JOB ETL USSTOCK ALPHA191 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_usstock_alpha101_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL USSTOCK ALPHA101 ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    data = QA_fetch_usstock_alpha101_adv(codes, start_date, end_date).data[['alpha001','alpha002','alpha003','alpha004','alpha005','alpha006',
                                                                            'alpha007','alpha008','alpha009','alpha010','alpha011','alpha012',
                                                                            'alpha013','alpha014','alpha015','alpha016','alpha017','alpha018',
                                                                            'alpha019','alpha020','alpha021','alpha022','alpha023','alpha024',
                                                                            'alpha025','alpha026','alpha027','alpha028','alpha029','alpha030',
                                                                            'alpha031','alpha032','alpha033','alpha034','alpha035','alpha036',
                                                                            'alpha037','alpha038','alpha039','alpha040','alpha041','alpha042',
                                                                            'alpha043','alpha044','alpha045','alpha046','alpha047','alpha049',
                                                                            'alpha050','alpha051','alpha052','alpha053','alpha054','alpha055',
                                                                            'alpha057','alpha060','alpha061','alpha062','alpha064','alpha065',
                                                                            'alpha066','alpha068','alpha071','alpha072','alpha073','alpha074',
                                                                            'alpha075','alpha077','alpha078','alpha081','alpha083','alpha085',
                                                                            'alpha086','alpha088','alpha092','alpha094','alpha095','alpha096',
                                                                            'alpha098','alpha099','alpha101']]
    if data is None:
        QA_util_log_info('##JOB NO USSTOCK ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "usstock_alpha101",if_exists='append')
        QA_util_log_info('##JOB ETL USSTOCK ALPHA101 HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_usstock_technical_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL USSTOCK TECHNICAL ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    data = QA_fetch_usstock_technical_index_adv(codes, start_date, end_date).data
    if data is None:
        QA_util_log_info(
            '##JOB NO USSTOCK TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "usstock_technical",if_exists='append')
        QA_util_log_info('##JOB ETL USSTOCK TECHNICAL HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_usstock_technical_week(start_date = QA_util_today_str(), end_date= None,ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info('##JOB Now ETL USSTOCK TECHNICAL WEEK ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    data = QA_fetch_usstock_technical_index_adv(codes, start_date, end_date,type='week').data
    if data is None:
        QA_util_log_info(
            '##JOB NO USSTOCK TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "usstock_technical_week",if_exists='append')
        QA_util_log_info('##JOB ETL USSTOCK TECHNICAL WEEK HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)

def QA_etl_usstock_financial_percent_day(start_date = QA_util_today_str(), end_date= None, ui_log= None):
    if end_date is None:
        end_date = QA_util_today_str()
    QA_util_log_info(
        '##JOB Now ETL USSTOCK FINANCIAL PERCENT ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    codes = list(QA_fetch_usstock_list()['code'])
    data = QA_fetch_usstock_financial_percent_adv(codes, start_date, end_date).data
    if data is None:
        QA_util_log_info(
            '##JOB NO USSTOCK FINANCIAL PERCENT HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)
    else:
        data = data.reset_index()
        data = data.assign(date=data.date.apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d')))
        QA_util_sql_store_mysql(data, "usstock_quant_financial_percent",if_exists='append')
        QA_util_log_info(
            '##JOB ETL USSTOCK FINANCIAL PERCENT HAS BEEN SAVED ==== from {from_} to {to_}'.format(from_=start_date,to_=end_date), ui_log)