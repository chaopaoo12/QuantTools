

import logging
import strategyease_sdk
from QUANTTOOLS.account_manage.setting import yun_ip, yun_port, easytrade_password


def get_Client(host=yun_ip, port=yun_port, key=easytrade_password):
    logging.basicConfig(level=logging.DEBUG)
    client = strategyease_sdk.Client(host=host, port=port, key=key)
    return(client)
