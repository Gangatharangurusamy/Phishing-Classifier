import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn_pandas import CategoricalImputer


class Preprocessor:
    """
    This class shall be used to clean and transform the data before training.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self, data, columns):
        """
        Method Name: remove_columns
        Description: This method removes the given columns from a pandas dataframe.
        Output: A pandas DataFrame after removing the specified columns.
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the remove_columns method of the Preprocessor class')
        try:
            useful_data = data.drop(labels=columns, axis=1)  # drop the labels specified in the columns
            self.logger_object.log(self.file_object,
                                   'Column removal Successful. Exited the remove_columns method of the Preprocessor class')
            return useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred in remove_columns method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object,
                                   'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise Exception()

    def separate_label_feature(self, data, label_column_name):
        """
        Method Name: separate_label_feature
        Description: This method separates the features and a Label Columns.
        Output: Returns two separate Dataframes, one containing features and the other containing Labels.
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            X = data.drop(labels=label_column_name, axis=1)  # drop the label column and keep features
            Y = data[label_column_name]  # keep the label column
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return X, Y
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred in separate_label_feature method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object,
                                   'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def drop_unnecessary_columns(self, data, column_name_list):
        """
        Method Name: drop_unnecessary_columns
        Description: This method drops the unwanted columns as discussed in the EDA section.
        Output: DataFrame after dropping unwanted columns.
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        return data.drop(column_name_list, axis=1)

    def replace_invalid_values_with_null(self, data):
        """
        Method Name: replace_invalid_values_with_null
        Description: This method replaces invalid values i.e., '?' with null, as discussed in EDA.
        Output: DataFrame with invalid values replaced by null.
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        for column in data.columns:
            data[column] = data[column].replace('?', np.nan)
        return data

    def is_null_present(self, data):
        """
        Method Name: is_null_present
        Description: This method checks whether there are null values present in the pandas DataFrame or not.
        Output: Returns True if null values are present, False if not and returns the list of columns with null values.
        On Failure: Raise Exception
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        null_present = False
        cols_with_missing_values = []
        try:
            null_counts = data.isna().sum()
            for i in range(len(null_counts)):
                if null_counts[i] > 0:
                    null_present = True
                    cols_with_missing_values.append(data.columns[i])
            if null_present:
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv')
            self.logger_object.log(self.file_object,
                                   'Finding missing values is a success. Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return null_present, cols_with_missing_values
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred in is_null_present method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object,
                                   'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def encode_categorical_values(self, data):
        """
        Method Name: encode_categorical_values
        Description: This method encodes all the categorical values in the training set.
        Output: DataFrame with categorical values encoded.
        On Failure: Raise Exception
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        data["class"] = data["class"].map({'p': 1, 'e': 2})
        data = pd.get_dummies(data)
        return data

    def encode_categorical_values_prediction(self, data):
        """
        Method Name: encode_categorical_values_prediction
        Description: This method encodes all the categorical values in the prediction set.
        Output: DataFrame with categorical values encoded.
        On Failure: Raise Exception
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        data = pd.get_dummies(data)
        return data

    def impute_missing_values(self, data, cols_with_missing_values):
        """
        Method Name: impute_missing_values
        Description: This method replaces all the missing values in the DataFrame using KNN Imputer.
        Output: DataFrame with missing values imputed.
        On Failure: Raise Exception
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of the Preprocessor class')
        try:
            imputer = CategoricalImputer()
            for col in cols_with_missing_values:
                data[col] = imputer.fit_transform(data[col])
            self.logger_object.log(self.file_object,
                                   'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred in impute_missing_values method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object,
                                   'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def get_columns_with_zero_std_deviation(self, data):
        """
        Method Name: get_columns_with_zero_std_deviation
        Description: This method finds out the columns which have a standard deviation of zero.
        Output: List of the columns with standard deviation of zero.
        On Failure: Raise Exception
        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object,
                               'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        try:
            col_to_drop = [col for col in data.columns if data[col].std() == 0]
            self.logger_object.log(self.file_object,
                                   'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return col_to_drop
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   f'Exception occurred in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object,
                                   'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()
