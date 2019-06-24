import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import (accuracy_score,confusion_matrix,
                             classification_report,roc_curve,roc_auc_score,
                             auc,precision_score,recall_score,f1_score)
from QUANTAXIS.QAUtil import QA_util_code_tolist

class model_ver1():

    def __init__(self, data, start_date, end_date, mark, test_size =0.01):
        self.data = data
        self.mark = QA_util_code_tolist(mark)
        self.rng = pd.Series(pd.date_range(start_date, end_date, freq='D')).apply(lambda x: str(x)[0:10])
        self.rng_train, self.rng_test = self.train_test_split(self.rng, test_size)

    def predict(self, start_date, end_date, mark):
        pred_rng = pd.Series(pd.date_range(start_date, end_date, freq='D')).apply(lambda x: str(x)[0:10])
        if mark != 'star' and 'mark' not in list(self.data.columns):
            print('must train star first!')
        else:
            cols = [i for i in self.data.columns if i not in ['moon','venus','mars','sun','star','TARGET','TARGET3','TARGET5','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5']]
            if mark not in self.mark:
                print("mark should been set in ('moon','venus','mars','sun') ")
            elif mark == 'moon':
                return(self.moon.model.predict_proba(self.data.loc[pred_rng][cols]))
            elif mark == 'venus':
                return(self.venus.model.predict_proba(self.data.loc[pred_rng][cols]))
            elif mark == 'mars':
                return(self.mars.model.predict_proba(self.data.loc[pred_rng][cols]))
            elif mark == 'sun':
                return(self.sun.model.predict_proba(self.data.loc[pred_rng][cols]))


    def model_p(self, model, mark):
        if mark != 'star' and 'mark' not in list(self.data.columns):
            print('must train star first!')
        else:
            cols = [i for i in self.data.columns if i not in ['moon','venus','mars','sun','star','TARGET','TARGET3','TARGET5','AVG_TARGET','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET5']]
            model.fit(self.data.loc[self.rng_train][cols],self.data.loc[self.rng_train][mark])
            y_pred = model.predict(self.data.loc[self.rng_train][cols])
            y_pred_test = model.predict(self.data.loc[self.rng_test][cols])
            accuracy_train = accuracy_score(self.data.loc[self.rng_train][mark],y_pred)
            accuracy_test = accuracy_score(self.data.loc[self.rng_test][mark],y_pred_test)
            precision_train= precision_score(self.data.loc[self.rng_train][mark],y_pred)
            precision_test = precision_score(self.data.loc[self.rng_test][mark], y_pred_test)
            summary = pd.DataFrame({'data_sub':['train','test'],'accuracy':[accuracy_train,accuracy_test],'precision':[precision_train,precision_test]})
            summary['mark'] = mark
            train_report = self.model_report(self.data.loc[self.rng_train][mark],y_pred,'train')
            test_report = self.model_report(self.data.loc[self.rng_test][mark],y_pred_test,'test')
            train_cross = pd.crosstab(self.data.loc[self.rng_test][mark], y_pred_test)
            test_cross = pd.crosstab(self.data.loc[self.rng_test][mark], y_pred_test)
            if mark == 'star':
                self.data['mark'] = model.predict(self.data[cols])
                self.star.model = model
                self.star.summary = summary
                self.star.report = [train_report, test_report]
                self.star.crosstab = [train_cross, test_cross]
            elif mark == 'moon':
                self.moon.model = model
                self.moon.summary = summary
                self.moon.report = [train_report, test_report]
                self.star.crosstab = [train_cross, test_cross]
            elif mark == 'venus':
                self.venus.model = model
                self.venus.summary = summary
                self.venus.report = [train_report, test_report]
                self.star.crosstab = [train_cross, test_cross]
            elif mark == 'mars':
                self.mars.model = model
                self.mars.summary = summary
                self.mars.report = [train_report, test_report]
                self.star.crosstab = [train_cross, test_cross]
            elif mark == 'sun':
                self.sun.model = model
                self.sun.summary = summary
                self.sun.report = [train_report, test_report]
                self.star.crosstab = [train_cross, test_cross]

    def training(self, model, mark):
        print('mark star first')
        model1 = XGBClassifier()
        self.model_p(model1, 'star')
        print('star has been set')
        for i in mark:
            print('mark i on training')
            self.model_p(model, i)

    def train_test_split(x,test_size=0.1):
        split_row = len(x) - int(test_size * len(x))
        x_train = x.iloc[:split_row]
        x_test = x.iloc[split_row:]
        return x_train,x_test

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

