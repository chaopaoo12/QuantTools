import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import (accuracy_score,
                             classification_report,
                             precision_score)
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_norm
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Model.QABaseModel.QAStockModel import QAStockModel
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_code_old

class QAStockXGBoost(QAStockModel):

    def build_model(self, other_params):
        QA_util_log_info('##JOB Set Model Params ===== {}'.format(self.info['date']), ui_log = None)
        self.info['other_params'] = other_params
        self.model = XGBClassifier(**self.info['other_params'])

    def model_running(self):
        QA_util_log_info('##JOB Now Model Traning ===== {}'.format(self.info['date']), ui_log = None)
        self.model.fit(self.X_train,self.Y_train)

        QA_util_log_info('##JOB Now Model Scoring ===== {}'.format(self.info['date']), ui_log = None)
        y_pred = self.model.predict(self.X_train)

        accuracy_train = accuracy_score(self.Y_train,y_pred)

        print("accuracy_train:"+str(accuracy_train)+"; precision_score On Train:"+str(precision_score(self.Y_train,y_pred)))
        self.train_report = classification_report(self.Y_train,y_pred, output_dict=True)
        print(self.train_report)
        self.info['train_report'] = self.train_report

    def model_important(self):
        QA_util_log_info('##JOB Now Got Model Importance ===== {}'.format(self.info['date']), ui_log = None)
        self.info['importance'] = pd.DataFrame({'featur' :self.info['cols'],'value':list(self.model.feature_importances_)}
                                  ).sort_values(by='value',ascending=False)
        return(self.info['importance'])

if __name__ == 'main':
    pass
