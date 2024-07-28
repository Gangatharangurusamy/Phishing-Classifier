import pickle
import os
from application_logging.logger import App_Logger

class File_Operation:
    """
    This class handles operations related to saving, loading, and finding machine learning models.
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory = 'models/'

    def save_model(self, model, filename):
        """
        Saves the model object to a file using pickle.

        Parameters:
        model: The machine learning model object to save.
        filename (str): The name of the file to save the model.

        Returns:
        str: 'success' if model is saved successfully.

        Raises:
        Exception: If saving the model fails.
        """
        self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory, filename)
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path)
            with open(os.path.join(path, filename + '.sav'), 'wb') as f:
                pickle.dump(model, f)
            self.logger_object.log(self.file_object, f'Model File {filename} saved.')
            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in save_model method: {str(e)}')
            raise e

    def load_model(self, filename):
        """
        Loads a model from the specified file.

        Parameters:
        filename (str): The name of the file containing the saved model.

        Returns:
        obj: The loaded machine learning model object.

        Raises:
        Exception: If loading the model fails.
        """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the File_Operation class')
        try:
            with open(os.path.join(self.model_directory, filename, filename + '.sav'), 'rb') as f:
                loaded_model = pickle.load(f)
            self.logger_object.log(self.file_object, f'Model File {filename} loaded.')
            return loaded_model
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in load_model method: {str(e)}')
            raise e

    def find_correct_model_file(self, cluster_number):
        """
        Finds the correct model file based on the cluster number.

        Parameters:
        cluster_number (int): The cluster number to search for in model filenames.

        Returns:
        str: The name of the correct model file.

        Raises:
        Exception: If no model file is found for the given cluster number.
        """
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.folder_name = self.model_directory
            self.list_of_model_files = os.listdir(self.folder_name)
            for self.file in self.list_of_model_files:
                if str(cluster_number) in self.file:
                    self.model_name = self.file.split('.')[0]
                    self.logger_object.log(self.file_object, 'Exited the find_correct_model_file method of the File_Operation class.')
                    return self.model_name
            raise Exception(f"No model file found for cluster number {cluster_number}")
        except Exception as e:
            self.logger_object.log(self.file_object, f'Exception occurred in find_correct_model_file method: {str(e)}')
            raise e
