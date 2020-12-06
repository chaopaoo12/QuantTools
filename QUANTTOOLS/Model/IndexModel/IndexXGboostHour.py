import pandas as pd
from xgboost import XGBClassifier
from QUANTTOOLS.Model.QABaseModel.QAIndexModelHour import QAIndexModelHour
from sklearn.metrics import (accuracy_score,classification_report,precision_score)
from QUANTAXIS.QAUtil import QA_util_log_info

class QAIndexXGBoostHour(QAIndexModelHour):

    def build_model(self, other_params):
        QA_util_log_info('##JOB Set Index Model Params ===== {}'.format(self.info['date']), ui_log = None)
        self.info['other_params'] = other_params
        self.model = XGBClassifier(**self.info['other_params'])

    def model_running(self):
        QA_util_log_info('##JOB Now Index Model Traning ===== {}'.format(self.info['date']), ui_log = None)
        self.model.fit(self.X_train,self.Y_train)

        QA_util_log_info('##JOB Now Index Model Scoring ===== {}'.format(self.info['date']), ui_log = None)
        y_pred = self.model.predict(self.X_train)

        accuracy_train = accuracy_score(self.Y_train,y_pred)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        self.info['train_report'] = self.train_report

    def model_important(self):
        QA_util_log_info('##JOB Now Got Index Model Importance ===== {}'.format(self.info['date']), ui_log = None)
        self.info['importance'] = pd.DataFrame({'featur' :self.info['cols'],'value':list(self.model.feature_importances_)}
                                               ).sort_values(by='value',ascending=False)
        return(self.info['importance'])

