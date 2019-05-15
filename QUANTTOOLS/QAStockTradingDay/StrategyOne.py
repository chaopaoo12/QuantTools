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

from sklearn.model_selection import GridSearchCV
from sklearn.externals import joblib
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score


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

def RandomForest():
    pass

def GBDT():
    pass


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