import sklearn.neural_network as sk_nn
import sklearn.neighbors as KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from sklearn.model_selection import GridSearchCV
from sklearn.externals import joblib
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score
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
KMeans(n_clusters=2, random_state=0).fit_predict(X)
KMeans(n_clusters=4, random_state=0).fit_predict(X)
KMeans(n_clusters=8, random_state=0).fit_predict(X)
KMeans(n_clusters=16, random_state=0).fit_predict(X)

#t-sne
X_tsne = TSNE(learning_rate=100).fit_transform(X)
#####base model (20)
#randomforest
rf0 = RandomForestClassifier(oob_score=True, random_state=10)
np.mean(cross_val_score(rf0, X, y, cv=10))
#knn(2,3,4,5,6,7,8) 10
knn_classifier=KNeighborsClassifier(2)
knn_classifier=KNeighborsClassifier(4)
knn_classifier=KNeighborsClassifier(8)
knn_classifier=KNeighborsClassifier(16)
knn_classifier=KNeighborsClassifier(32)
knn_classifier=KNeighborsClassifier(64)
knn_classifier=KNeighborsClassifier(128)
knn_classifier=KNeighborsClassifier(256)
knn_classifier=KNeighborsClassifier(512)
knn_classifier=KNeighborsClassifier(1024)

knn_classifier.fit(X,y)

#GBDT
gbm0 = GradientBoostingClassifier(random_state=10)
gbm0.fit(X,y)

param_test1 = {'n_estimators':range(20,81,10)}
gsearch1 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, min_samples_split=300,
                                                               min_samples_leaf=20,max_depth=8,max_features='sqrt', subsample=0.8,random_state=10),
                        param_grid = param_test1, scoring='roc_auc',iid=False,cv=5)
gsearch1.fit(X,y)
gsearch1.grid_scores_, gsearch1.best_params_, gsearch1.best_score_

param_test2 = {'max_depth':range(3,14,2), 'min_samples_split':range(100,801,200)}
gsearch2 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60, min_samples_leaf=20,
                                                               max_features='sqrt', subsample=0.8, random_state=10),
                        param_grid = param_test2, scoring='roc_auc',iid=False, cv=5)
gsearch2.fit(X,y)
gsearch2.grid_scores_, gsearch2.best_params_, gsearch2.best_score_

param_test3 = {'min_samples_split':range(800,1900,200), 'min_samples_leaf':range(60,101,10)}
gsearch3 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7,
                                                               max_features='sqrt', subsample=0.8, random_state=10),
                        param_grid = param_test3, scoring='roc_auc',iid=False, cv=5)
gsearch3.fit(X,y)
gsearch3.grid_scores_, gsearch3.best_params_, gsearch3.best_score_

#XGBOOST
#ExtraTreesClassifier
clf = ExtraTreesClassifier(n_estimators=10, max_depth=None,min_samples_split=2, random_state=0)
clf.fit(X, y)
#AdaBoostClassifier
clf = AdaBoostClassifier(n_estimators=100, random_state=0)
clf.fit(X, y)
#BaggingClassifier
clf = BaggingClassifier(n_estimators=100, random_state=0)
clf.fit(X, y)
#neural_network
model = sk_nn.MLPClassifier(activation='tanh',solver='adam',alpha=0.0001,learning_rate='adaptive',learning_rate_init=0.001,max_iter=200)
model = sk_nn.MLPClassifier(solver='lbfgs', alpha=1e-5,hidden_layer_sizes=(15,), random_state=1)
model.fit(X, y)

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