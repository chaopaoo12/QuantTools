import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch.QAQuery_Advance import QA_fetch_stock_quant_pre_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_target,QA_fetch_get_quant_data
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
import keras
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers import LSTM
from keras import backend as K
from keras.metrics import top_k_categorical_accuracy
import tensorflow as tf
#from tensorflow.keras.metrics import top_k_categorical_accuracy
import numpy as np
import QUANTAXIS as QA

def get_quant_data(start_date, end_date, type = 'crawl', block = False, sub_block= True):
    if block is True:
        data = QA.QA_fetch_stock_block()
        codes = list(data[data.blockname.isin(['上证50','沪深300','创业300','上证180','上证380','深证100','深证300','中证100','中证200'])]['code'].drop_duplicates())
        #codes = [i for i in codes if i.startswith('300') == False]
    else:
        codes = list(QA_fetch_stock_list_adv()['code'])
    if type == 'crawl':
        res = QA_fetch_stock_quant_pre_adv(codes,start_date,end_date, sub_block).data
    if type == 'model':
        res = QA_fetch_get_quant_data(codes, start_date, end_date, sub_block).set_index(['date','code']).drop(['date_stamp'], axis=1)
        target = QA_fetch_stock_target(codes, start_date, end_date)
        res = res.join(target)
    #res = res[(res['RNG_L_O'] <= 5 & res['LAG_TOR_O'] < 1)]
    #dummy_industry = pd.get_dummies(res['INDUSTRY']).astype(float)
    #dummy_industry.columns = ['I_' + i for i in list(dummy_industry.columns)]
    #res = pd.concat([res[[col for col in list(res.columns) if col != 'INDUSTRY']],dummy_industry],axis = 1)
    return(res)

def precision(y_true, y_pred):
    # Calculates the precision
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

def recall(y_true, y_pred):
    # Calculates the recall
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
    recall = true_positives / (possible_positives + K.epsilon())
    return recall

def auc(y_true, y_pred):
    ptas = tf.stack([binary_PTA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)],axis=0)
    pfas = tf.stack([binary_PFA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)],axis=0)
    pfas = tf.concat([tf.ones((1,)) ,pfas],axis=0)
    binSizes = -(pfas[1:]-pfas[:-1])
    s = ptas*binSizes
    return K.sum(s, axis=0)

# PFA, prob false alert for binary classifier
def binary_PFA(y_true, y_pred, threshold=K.variable(value=0.5)):
    y_pred = K.cast(y_pred >= threshold, 'float32')
    # N = total number of negative labels
    N = K.sum(1 - y_true)
    # FP = total number of false alerts, alerts from the negative class labels
    FP = K.sum(y_pred - y_pred * y_true)
    return FP/N

# P_TA prob true alerts for binary classifier
def binary_PTA(y_true, y_pred, threshold=K.variable(value=0.5)):
    y_pred = K.cast(y_pred >= threshold, 'float32')
    # P = total number of positive labels
    P = K.sum(y_true)
    # TP = total number of correct alerts, alerts from the positive class labels
    TP = K.sum(y_pred * y_true)
    return TP/P

def f1_loss(y_true, y_pred):
    #计算tp、tn、fp、fn
    tp = K.sum(K.cast(y_true*y_pred, 'float'), axis=0)
    tn = K.sum(K.cast((1-y_true)*(1-y_pred), 'float'), axis=0)
    fp = K.sum(K.cast((1-y_true)*y_pred, 'float'), axis=0)
    fn = K.sum(K.cast(y_true*(1-y_pred), 'float'), axis=0)
    #percision与recall，这里的K.epsilon代表一个小正数，用来避免分母为零
    p = tp / (tp + fp + K.epsilon())
    r = tp / (tp + fn + K.epsilon())
    #计算f1
    f1 = 1.01 * p * r / (0.01 * p + 1 * r + K.epsilon())
    f1 = tf.where(tf.is_nan(f1), tf.zeros_like(f1), f1)#其实就是把nan换成0
    return 1 - K.mean(f1)

def train_test_split_date(x, test_size=0.3):
    split_row = len(x) - int(test_size * len(x))
    x_train = x.iloc[:split_row]
    x_test = x.iloc[split_row:]
    return x_train, x_test

def train_test_split_date(x, test_size=0.3):
    ind = np.arange(len(x))
    np.random.shuffle(x)
    split_row = len(x) - int(test_size * len(x))
    x_train = x[ind[:split_row]]
    x_test = x[ind[split_row:]]
    return x_train, x_test

def mkdir(path):
    # 引入模块
    import os

    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        try:
            os.makedirs(path)

            print(path + ' 创建成功')
            return True
        except:
            return False
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')
        return True