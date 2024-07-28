from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

class Model_Finder:
    """
    This class shall be used to find the model with the best accuracy and AUC score.

    Written By: iNeuron Intelligence
    Version: 2.0
    Revisions: Improved readability and exception handling
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.sv_classifier = SVC()
        self.xgb = XGBClassifier(objective='binary:logistic', n_jobs=-1)

    def get_best_params_for_svm(self, train_x, train_y):
        """
        Method Name: get_best_params_for_svm
        Description: Get the parameters for the SVM Algorithm which give the best accuracy using Hyper Parameter Tuning.
        Output: The model with the best parameters
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 2.0
        Revisions: Improved readability and exception handling
        """
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_svm method of the Model_Finder class')
        try:
            param_grid = {
                "kernel": ['rbf', 'sigmoid'],
                "C": [0.1, 0.5, 1.0],
                "random_state": [0, 100, 200, 300]
            }
            grid = GridSearchCV(estimator=self.sv_classifier, param_grid=param_grid, cv=5, verbose=3)
            grid.fit(train_x, train_y)

            best_params = grid.best_params_
            self.sv_classifier = SVC(kernel=best_params['kernel'], C=best_params['C'], random_state=best_params['random_state'])
            self.sv_classifier.fit(train_x, train_y)
            self.logger_object.log(self.file_object, f'SVM best params: {best_params}. Exited the get_best_params_for_svm method of the Model_Finder class')

            return self.sv_classifier
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in get_best_params_for_svm method of the Model_Finder class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object, 'SVM training failed. Exited the get_best_params_for_svm method of the Model_Finder class')
            raise Exception(f'Error in get_best_params_for_svm method: {str(e)}')

    def get_best_params_for_xgboost(self, train_x, train_y):
        """
        Method Name: get_best_params_for_xgboost
        Description: Get the parameters for XGBoost Algorithm which give the best accuracy using Hyper Parameter Tuning.
        Output: The model with the best parameters
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 2.0
        Revisions: Improved readability and exception handling
        """
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            param_grid_xgboost = {
                "n_estimators": [100, 130],
                "max_depth": range(8, 10, 1)
            }
            grid = GridSearchCV(XGBClassifier(objective='binary:logistic'), param_grid_xgboost, verbose=3, cv=5)
            grid.fit(train_x, train_y)

            best_params = grid.best_params_
            self.xgb = XGBClassifier(n_estimators=best_params['n_estimators'], max_depth=best_params['max_depth'], n_jobs=-1)
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object, f'XGBoost best params: {best_params}. Exited the get_best_params_for_xgboost method of the Model_Finder class')

            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in get_best_params_for_xgboost method of the Model_Finder class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object, 'XGBoost parameter tuning failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception(f'Error in get_best_params_for_xgboost method: {str(e)}')

    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
        Method Name: get_best_model
        Description: Find out the model which has the best AUC score.
        Output: The best model name and the model object
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 2.0
        Revisions: Improved readability and exception handling
        """
        self.logger_object.log(self.file_object, 'Entered the get_best_model method of the Model_Finder class')
        try:
            xgboost_model = self.get_best_params_for_xgboost(train_x, train_y)
            xgboost_prediction = xgboost_model.predict(test_x)

            if len(test_y.unique()) == 1:
                xgboost_score = accuracy_score(test_y, xgboost_prediction)
                self.logger_object.log(self.file_object, f'Accuracy for XGBoost: {xgboost_score}')
            else:
                xgboost_score = roc_auc_score(test_y, xgboost_prediction)
                self.logger_object.log(self.file_object, f'AUC for XGBoost: {xgboost_score}')

            svm_model = self.get_best_params_for_svm(train_x, train_y)
            svm_prediction = svm_model.predict(test_x)

            if len(test_y.unique()) == 1:
                svm_score = accuracy_score(test_y, svm_prediction)
                self.logger_object.log(self.file_object, f'Accuracy for SVM: {svm_score}')
            else:
                svm_score = roc_auc_score(test_y, svm_prediction)
                self.logger_object.log(self.file_object, f'AUC for SVM: {svm_score}')

            if svm_score < xgboost_score:
                self.logger_object.log(self.file_object, 'XGBoost is the best model')
                return 'XGBoost', xgboost_model
            else:
                self.logger_object.log(self.file_object, 'SVM is the best model')
                return 'SVM', svm_model

        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in get_best_model method of the Model_Finder class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object, 'Model selection failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception(f'Error in get_best_model method: {str(e)}')
