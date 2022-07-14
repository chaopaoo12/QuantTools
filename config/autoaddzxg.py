from QUANTTOOLS.Message.message_func.email import send_email, reademail
from QUANTTOOLS.Ananlysis.Tools.addtdxZXG import putzxgfile
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_get_pre_trade_date


if __name__ == '__main__':

    if QA_util_if_trade(QA_util_today_str()):
        check_day = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        htmlbody=reademail('目标股池{}'.format(check_day))
        data = pd.read_html(htmlbody[0], encoding='gbk', header=0)[1]
        data=data[data.RANK.notna()].rename(columns={'Unnamed: 0':'code'})
        putzxgfile(data.code.unique().tolist())