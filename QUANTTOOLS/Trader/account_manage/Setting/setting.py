from QUANTAXIS.QAUtil.QASetting import QA_Setting

QASETTING = QA_Setting()

yun_ip = QASETTING.get_config('EASYTRADE','ip')
yun_port = QASETTING.get_config('EASYTRADE','port')
easytrade_password = QASETTING.get_config('EASYTRADE','password')