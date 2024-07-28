import pandas as pd

class Data_Getter:
    """
    This class shall be used for obtaining the data from the source for training.

    Written By: iNeuron Intelligence
    Version: 2.0
    Revisions: Improved readability and exception handling
    """
    def __init__(self, file_object, logger_object):
        self.training_file = 'Training_FileFromDB/InputFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from the source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 2.0
        Revisions: Improved readability and exception handling
        """
        self.logger_object.log(self.file_object, 'Entered the get_data method of the Data_Getter class')
        try:
            data = pd.read_csv(self.training_file)  # reading the data file
            self.logger_object.log(self.file_object, 'Data Load Successful. Exited the get_data method of the Data_Getter class')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in get_data method of the Data_Getter class. Exception message: {str(e)}')
            self.logger_object.log(self.file_object, 'Data Load Unsuccessful. Exited the get_data method of the Data_Getter class')
            raise Exception(f'Error in get_data method: {str(e)}')
