import sklearn.neural_network as sk_nn
import sklearn.neighbors as sk_neighbors
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import AdaBoostClassifier as ADA
from sklearn.ensemble import BaggingClassifier
from sklearn.manifold import t_sne
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from sklearn.model_selection import GridSearchCV
from sklearn.externals import joblib
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import pandas as pd

from sklearn.metrics import (accuracy_score,confusion_matrix,
                             classification_report,roc_curve,roc_auc_score,
                             auc,precision_score,recall_score,f1_score)

import time
from functools import wraps
def time_this_function(func):
    #作为装饰器使用，返回函数执行需要花费的时间
    @wraps(func)
    def wrapper(*args,**kwargs):
        start=time.time()
        result=func(*args,**kwargs)
        end=time.time()
        print(func.__name__,end-start)
        return result

#####prepare
#Scale( Log(X+1) )
#Log(X+1)
#Scale( X )
#X
#sqrt( X + 3/8)
#kmeans
#t-sne

#####base model (20)
#randomforest
#knn(2,3,4,5,6,7,8) 10
#GBDT
#XGBOOST
#ExtraTreesClassifier
#AdaBoostClassifier
#BaggingClassifier
#neural_network

#####lv2 model
#AdaBoostClassifier
#TF
#XGBOOST

#####submit model
#LogisticRegression

from sklearn.metrics import confusion_matrix
def customedscore(preds, dtrain):
    label = dtrain.get_label()
    pred = [int(i>=0.5) for i in preds]
    confusion_matrixs = confusion_matrix(label, pred)
    recall =float(confusion_matrixs[0][0]) / float(confusion_matrixs[0][1]+confusion_matrixs[0][0])
    precision = float(confusion_matrixs[0][0]) / float(confusion_matrixs[1][0]+confusion_matrixs[0][0])
    F = 5*precision* recall/(2*precision+3*recall)*100
    return 'FSCORE',float(F)

def model_report(y, y_pred, label):
    report = classification_report(y, y_pred)
    import re
    s = [x for x in re.split("\s{2}|\n", report)  if len(x) > 0]
    s.insert(0,"cols")
    report = pd.DataFrame([s[i:i+int(5)] for i in range(0,len(s),int(5))])
    report1 = pd.DataFrame(report.iloc[1:,:])
    report1.columns = list(report.iloc[0,:])
    report1[label] = label
    return(report1)

@time_this_function
def RandomForest(x_train,y_train,x_dev,y_dev,x_test,y_test,x_test2,y_test2):
    rf0 = RandomForestClassifier(oob_score=True, random_state=10)
    rf0.fit(x_train,y_train)
    rf0.predict(x_dev)
    rf0.predict(x_dev)
    rf0.predict(x_dev)

    return(rf0.predict(x_train))

@time_this_function
def GBDT():
    pass

@time_this_function
def xgboost(x_train,y_train,x_dev,y_dev,x_test,y_test,x_test2,y_test2):
    model = XGBClassifier()
    model.fit(x_train,y_train)
    res = model.predict(x_train)
    train = model_report(y_train,res,'train')
    dev = model_report(y_dev,model.predict(x_dev),'dev')
    test = model_report(y_test,model.predict(x_test),'test')
    test2 = model_report(y_test2,model.predict(x_test2),'test2')
    dic = {'train':train,'dev': dev,'test':test,'test':test2}
    return(list(model,dic))

@time_this_function
def preparation(quant):
    dummies=pd.get_dummies(quant['INDUSTRY'],prefix='INDUSTRY')
    quant=quant[[x for x in list(quant.columns) if x != "INDUSTRY"]].join(dummies)
    quant['moon']=quant.TARGET.apply(lambda x: 1 if  x >= 7 else 0)
    quant['sun']=quant.TARGET.apply(lambda x: 1 if  x >= 9 else 0)
    quant['star']=quant.TARGET.apply(lambda x: 1 if  x > 0 else 0)
    return(quant)

@time_this_function
def train_test_split(quant, train_rng, test_rng, target):
    cols = [i for i in quant.columns if i not in ['moon','sun','star','TARGET','TARGET3','TARGET5']]
    rng1 = pd.Series(pd.date_range(train_rng[0], train_rng[1], freq='D')).apply(lambda x: str(x)[0:10])
    rng2 = pd.Series(pd.date_range(test_rng[0], test_rng[1], freq='D')).apply(lambda x: str(x)[0:10])
    x_train,x_test_all,y_train,y_test_all = train_test_split(quant.loc[rng1][cols],quant.loc[rng1][target],test_size=0.3,random_state=9)
    x_dev,x_test,y_dev,y_test = train_test_split(x_test_all,y_test_all,test_size=0.3,random_state=9)
    y_test2=quant.loc[rng2][target]
    x_test2 = quant.loc[rng2][cols]
    return(x_train,x_dev,x_test,x_test2,y_train,y_dev,y_test,y_test2)

@time_this_function
def predict(model,x,y=None):
    y_pred = model.predict(x)
    if y is not None:
        report = model_report(y, y_pred, 'pred')
    else:
        report = None
    return(list(y_pred,report))

class Stacking():

    def __init__(self, X_train, X_test, Y_train, Y_test):
        self.X_train = X_train
        self.X_test = X_test
        self.Y_train = Y_train
        self.Y_test = Y_test

    def prepare(self):
        pass

    def level_one(self):
        pass

    def level_two(self):
        pass

    def level_three(self):
        pass