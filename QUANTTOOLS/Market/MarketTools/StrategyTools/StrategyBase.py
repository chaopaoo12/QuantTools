from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import open_check, close_check, suspend_check, get_on_time,time_check_before, check_market_time,time_check_after
from QUANTTOOLS.QAStockETL.Crawly.IP_Proxy import get_ip_poll,check_ip_poll
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_tfp
from QUANTTOOLS.QAStockETL.QAUtil.base_func import except_output
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
import time
import datetime

class StrategyBase:

    def __init__(self, target_list=None, position=None, sub_account=None, base_percent=None, trading_date=None):
        self.target_list = target_list
        self.trading_date = trading_date
        self.position = position
        self.sub_account = sub_account
        self.base_percent = base_percent
        self.signal_func = None
        self.balance_func = None
        self.percent_func = None
        self.buy_list = None
        self.sec_temp_data = []
        self.day_temp_data = []
        self.source_data = None
        self.proxies = []

    def set_proxy_pool(self, url=None, ckeck_url=None):
        if url is not None:
            proxies = get_ip_poll(url)
        else:
            proxies = get_ip_poll()
        if ckeck_url is not None:
            self.proxies = [check_ip_poll(i, ckeck_url) for i in proxies]
        else:
            self.proxies = [check_ip_poll(i) for i in proxies]

    def set_code_check(self):
        QA_util_log_info('##JOB Check TFP stock  ==== {}'.format(self.trading_date), ui_log= None)
        tfp = QA_fetch_get_stock_tfp(self.trading_date)
        QA_util_log_info('##JOB Stock on TFP  ==== {}'.format(self.trading_date), ui_log= None)
        QA_util_log_info([i for i in self.target_list if i in tfp], ui_log= None)
        self.target_list = [i for i in self.target_list if i not in tfp]

    def set_init_func(self, func):
        self.init_func = func
        self.day_temp_data = []

    def set_signal_func(self, func, signaltime_list=None):
        self.signal_func = func
        self.signaltime_list = signaltime_list

    def set_codsel_func(self, func=None, codseltime_list=None):
        self.codsel_func = func
        self.codseltime_list = codseltime_list

    def set_balance_func(self, func):
        self.balance_func = func

    def set_percent_func(self, func=None):
        self.percent_func = func


    def code_select(self, mark_tm):
        QA_util_log_info('##JOB Refresh Proxy Pool  ==== {}'.format(mark_tm), ui_log= None)

        k = 0
        while k <= 2:
            try:
                self.set_proxy_pool()
                break
            except:
                k+=1

        QA_util_log_info('##JOB Refresh Tmp Code List  ==== {}'.format(mark_tm), ui_log= None)
        QA_util_log_info('##JOB Init Code List  ==== {}'.format(self.target_list), ui_log= None)
        if self.codsel_func is not None:
            #while k <= 2:
                #try:
            self.buy_list, self.sec_temp_data, self.source_data = self.codsel_func(target_list=self.target_list,
                                                                                   position=self.position,
                                                                                   day_temp_data=self.day_temp_data,
                                                                                   sec_temp_data=self.sec_temp_data,
                                                                                   trading_date=self.trading_date,
                                                                                   mark_tm=mark_tm,
                                                                                   proxies=self.proxies)
                #except:
                #    k+=1
        else:
            self.buy_list = None
            self.sec_temp_data = []
            self.source_data = None


    def init_run(self):
        if self.init_func is not None:
            if self.position is not None and self.position.shape[0] > 0:
                code_list = list(set(self.target_list + self.position.code.tolist()))
            else:
                code_list = self.target_list
            self.day_temp_data = self.init_func(code_list,
                                             self.trading_date)
        else:
            self.day_temp_data = []


    def signal_run(self, mark_tm):
        data = self.signal_func(target_list=self.target_list,
                                buy_list=self.buy_list,
                                position=self.position,
                                sec_temp_data=self.sec_temp_data,
                                day_temp_data=self.day_temp_data,
                                source_data=self.source_data,
                                trading_date=self.trading_date,
                                mark_tm=mark_tm,
                                proxies=self.proxies)
        return(data)


    def percent_run(self, mark_tm):
        if self.percent_func is not None:
            return self.percent_func(target_list = self.target_list,
                                     buy_list = self.buy_list,
                                     position = self.position,
                                     trading_date = self.trading_date,
                                     mark_tm = mark_tm)
        else:
            return self.base_percent


    def balance_run(self, signal_data, percent):
        return self.balance_func(signal_data, self.position, self.sub_account, percent)


    def strategy_run(self, mark_tm):

        QA_util_log_info('##JOB Now Start Trading ==== {}'.format(mark_tm), ui_log= None)

        if mark_tm in self.codseltime_list or len(self.sec_temp_data) == 0:

            # init codseltime
            #self.start_status = True
            tm = datetime.datetime.now().strftime("%H:%M:%S")
            codsel_tmmark = get_on_time(tm, self.codseltime_list)
            QA_util_log_info('##JOB Now Init Codselt Mark Time ==== {}'.format(str(codsel_tmmark)), ui_log=None)

            QA_util_log_info('JOB Selct Code List ==================== {}'.format(codsel_tmmark), ui_log=None)
            k = 0
            while k <= 2:
                QA_util_log_info('JOB Selct Code List {x} times ==================== '.format(x=k+1), ui_log=None)
                k += 1
                #try:
                self.code_select(codsel_tmmark)
                QA_util_log_info('JOB Selct Code List Success ==================== {}'.format(codsel_tmmark), ui_log=None)
                break
                #except:
                #    k += 1

            if k > 2:
                QA_util_log_info('JOB Selct Code List Failed ==================== ', ui_log=None)
        else:
            QA_util_log_info('JOB Init Source Data ==================== ', ui_log=None)
            self.source_data = None

        if mark_tm in self.signaltime_list:
            QA_util_log_info('JOB Init Trading Signal ==================== {}'.format(
                mark_tm), ui_log=None)
            k = 0
            while k <= 2:
                QA_util_log_info('JOB Get Trading Signal {x} times ==================== '.format(x=k+1), ui_log=None)
                data = self.signal_run(mark_tm)
                if data is None and self.buy_list is not None:
                    time.sleep(5)
                    k += 1
                else:
                    QA_util_log_info('JOB Init Trading Signal List ==================== {}'.format(mark_tm), ui_log=None)
                    break

            QA_util_log_info('JOB Init Capital Percent ==================== {}'.format(mark_tm), ui_log=None)
            percent = self.percent_run(mark_tm)

            QA_util_log_info('JOB Balance Stock Capital ==================== {}'.format(mark_tm), ui_log=None)
            balance_data = self.balance_run(data, percent)

            QA_util_log_info('JOB Return Signal Data ==================== {}'.format(mark_tm), ui_log=None)
            signal_data = build_info(balance_data)

            return(signal_data)


def build_info(data):
    # 数据规整化 固定的工具函数
    # 标准数据格式
    # signal = {'sell':[{'code':'000001', 代码
    #                   'hold':0, 持仓量
    #                   'name':'xx', 名称
    #                   'industry':'xxxx', 行业
    #                   'msg':'xxx', 消息
    #                   'close':0, 昨日收盘价 默认0 抢板可使用
    #                   'target_position':0, 分配比例
    #                   'target_capital':0,目标持仓金额 balance结果
    #                   'data':'xxxx'}, 原始数据
    #                  {...}],
    #          'buy':[{'code':'000004',
    #                  'hold':0,
    #                  'name':'xx',
    #                  'industry':'xxxx',
    #                  'msg':'xxx',
    #                  'close':0,
    #                  'target_position':0,
    #                  'target_capital':0,目标持仓金额
    #                  'data':'xxxx'},
    #                 {...}]}
    if data is not None:

        QA_util_log_info('##CHECK columns ', ui_log = None)
        need_columns = ['code', 'name', 'industry', 'msg', 'close', 'target_position', 'target_capital', 'mark']
        for inset_column in [i for i in need_columns if i not in data.columns]:
            QA_util_log_info('##CHECK short of columns {}'.format(inset_column), ui_log = None)
            data[inset_column] = 0

        sell_list = data[data.mark == 'sell'].code.tolist()
        sell_dict = data[data.code.isin(sell_list)][need_columns].to_dict(orient='records')
        buy_list = data[data.mark == 'buy'].code.tolist()
        buy_dict = data[data.code.isin(buy_list)][need_columns].to_dict(orient='records')

        signal_data = {'sell': sell_dict, 'buy': buy_dict}
        QA_util_log_info(signal_data, ui_log = None)
        return(signal_data)
    else:
        return({'sell': None, 'buy': None})


if __name__ == '__main__':
    pass