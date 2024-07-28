import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

class Prediction_Data_validation:
    """
    This class handles all validations on the raw prediction data.
    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """
        Extracts all the relevant information from the schema file.
        Returns: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
        Raises: ValueError, KeyError, Exception
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            log_message = (f"LengthOfDateStampInFile: {LengthOfDateStampInFile}, "
                           f"LengthOfTimeStampInFile: {LengthOfTimeStampInFile}, "
                           f"NumberofColumns: {NumberofColumns}")
            self.logger.log("Prediction_Logs/valuesfromSchemaValidationLog.txt", log_message)
            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

        except (ValueError, KeyError) as e:
            error_message = f"{type(e).__name__}: {str(e)}"
            self.logger.log("Prediction_Logs/valuesfromSchemaValidationLog.txt", error_message)
            raise e
        except Exception as e:
            self.logger.log("Prediction_Logs/valuesfromSchemaValidationLog.txt", str(e))
            raise e

    def manualRegexCreation(self):
        """
        Creates a regex pattern to validate the prediction data filenames.
        Returns: Regex pattern
        """
        return "['phising']+['\_'']+[\d_]+[\d]+\.csv"

    def createDirectoryForGoodBadRawData(self):
        """
        Creates directories to store good and bad data after validation.
        Raises: OSError
        """
        try:
            for dir_type in ["Good_Raw", "Bad_Raw"]:
                path = os.path.join("Prediction_Raw_Files_Validated", dir_type)
                if not os.path.isdir(path):
                    os.makedirs(path)
        except OSError as ex:
            self.logger.log("Prediction_Logs/GeneralLog.txt", f"Error while creating Directory: {str(ex)}")
            raise ex

    def deleteExistingGoodDataTrainingFolder(self):
        """
        Deletes the directory for good data if it exists.
        Raises: OSError
        """
        try:
            path = 'Prediction_Raw_Files_Validated/Good_Raw/'
            if os.path.isdir(path):
                shutil.rmtree(path)
                self.logger.log("Prediction_Logs/GeneralLog.txt", "GoodRaw directory deleted successfully!")
        except OSError as ex:
            self.logger.log("Prediction_Logs/GeneralLog.txt", f"Error while deleting directory: {str(ex)}")
            raise ex

    def deleteExistingBadDataTrainingFolder(self):
        """
        Deletes the directory for bad data if it exists.
        Raises: OSError
        """
        try:
            path = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(path):
                shutil.rmtree(path)
                self.logger.log("Prediction_Logs/GeneralLog.txt", "BadRaw directory deleted successfully!")
        except OSError as ex:
            self.logger.log("Prediction_Logs/GeneralLog.txt", f"Error while deleting directory: {str(ex)}")
            raise ex

    def moveBadFilesToArchiveBad(self):
        """
        Moves files from the bad data directory to an archive directory and deletes the original bad data directory.
        Raises: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            archive_path = os.path.join("PredictionArchivedBadData", f"BadData_{str(date)}_{str(time)}")
            if not os.path.isdir(archive_path):
                os.makedirs(archive_path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            for file in os.listdir(source):
                shutil.move(os.path.join(source, file), archive_path)
            self.logger.log("Prediction_Logs/GeneralLog.txt", "Bad files moved to archive")
            if os.path.isdir(source):
                shutil.rmtree(source)
            self.logger.log("Prediction_Logs/GeneralLog.txt", "Bad Raw Data Folder deleted successfully!")
        except OSError as ex:
            self.logger.log("Prediction_Logs/GeneralLog.txt", f"Error while moving bad files to archive: {str(ex)}")
            raise ex

    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
        Validates the filenames of prediction data files.
        Args:
            regex (str): The regex pattern for filename validation.
            LengthOfDateStampInFile (int): The expected length of the date stamp in the filename.
            LengthOfTimeStampInFile (int): The expected length of the time stamp in the filename.
        Raises: Exception
        """
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        try:
            log_file = "Prediction_Logs/nameValidationLog.txt"
            for filename in listdir(self.Batch_Directory):
                if re.match(regex, filename):
                    split_at_dot = re.split('.csv', filename)
                    split_at_dot = re.split('_', split_at_dot[0])
                    if len(split_at_dot[1]) == LengthOfDateStampInFile and len(split_at_dot[2]) == LengthOfTimeStampInFile:
                        shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Good_Raw")
                        self.logger.log(log_file, f"Valid File name! File moved to GoodRaw Folder: {filename}")
                    else:
                        shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(log_file, f"Invalid File name! File moved to Bad Raw Folder: {filename}")
                else:
                    shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(log_file, f"Invalid File name! File moved to Bad Raw Folder: {filename}")
        except Exception as e:
            self.logger.log(log_file, f"Error occurred while validating FileName: {str(e)}")
            raise e

