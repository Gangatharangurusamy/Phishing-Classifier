import sqlite3
from datetime import datetime
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

class Raw_Data_validation:

    """
    This class shall be used for handling all the validation done on the Raw Training Data!!

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None
    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """
        Method Name: valuesFromSchema
        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
        On Failure: Raise ValueError,KeyError,Exception
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            log_message = (
                f"LengthOfDateStampInFile:: {LengthOfDateStampInFile}\t"
                f"LengthOfTimeStampInFile:: {LengthOfTimeStampInFile}\t "
                f"NumberofColumns:: {NumberofColumns}\n"
            )
            self._log_message("Training_Logs/valuesfromSchemaValidationLog.txt", log_message)

        except (ValueError, KeyError) as e:
            self._log_message("Training_Logs/valuesfromSchemaValidationLog.txt", f"{type(e).__name__}: {str(e)}")
            raise e

        except Exception as e:
            self._log_message("Training_Logs/valuesfromSchemaValidationLog.txt", str(e))
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):
        """
        Method Name: manualRegexCreation
        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                     This Regex is used to validate the filename of the training data.
        Output: Regex pattern
        """
        return "['phising']+['\_'']+[\d_]+[\d]+\.csv"

    def createDirectoryForGoodBadRawData(self):
        """
        Method Name: createDirectoryForGoodBadRawData
        Description: This method creates directories to store the Good Data and Bad Data
                     after validating the training data.
        Output: None
        On Failure: OSError
        """
        try:
            good_raw_path = os.path.join("Training_Raw_files_validated", "Good_Raw")
            bad_raw_path = os.path.join("Training_Raw_files_validated", "Bad_Raw")

            if not os.path.exists(good_raw_path):
                os.makedirs(good_raw_path)
            if not os.path.exists(bad_raw_path):
                os.makedirs(bad_raw_path)

        except OSError as ex:
            self._log_message("Training_Logs/GeneralLog.txt", f"Error while creating Directory {ex}")
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
        Method Name: deleteExistingGoodDataTrainingFolder
        Description: This method deletes the directory made to store the Good Data
                     after loading the data in the table. Once the good files are
                     loaded in the DB, deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError
        """
        try:
            path = os.path.join('Training_Raw_files_validated', 'Good_Raw')
            if os.path.exists(path):
                shutil.rmtree(path)
                self._log_message("Training_Logs/GeneralLog.txt", "GoodRaw directory deleted successfully!!!")
        except OSError as s:
            self._log_message("Training_Logs/GeneralLog.txt", f"Error while Deleting Directory: {s}")
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        """
        Method Name: deleteExistingBadDataTrainingFolder
        Description: This method deletes the directory made to store the bad Data.
        Output: None
        On Failure: OSError
        """
        try:
            path = os.path.join('Training_Raw_files_validated', 'Bad_Raw')
            if os.path.exists(path):
                shutil.rmtree(path)
                self._log_message("Training_Logs/GeneralLog.txt", "BadRaw directory deleted before starting validation!!!")
        except OSError as s:
            self._log_message("Training_Logs/GeneralLog.txt", f"Error while Deleting Directory: {s}")
            raise OSError

    def moveBadFilesToArchiveBad(self):
        """
        Method Name: moveBadFilesToArchiveBad
        Description: This method deletes the directory made to store the Bad Data
                     after moving the data in an archive folder. We archive the bad
                     files to send them back to the client for invalid data issue.
        Output: None
        On Failure: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_files_validated/Bad_Raw'
            if os.path.exists(source):
                archive_path = "TrainingArchiveBadData"
                if not os.path.exists(archive_path):
                    os.makedirs(archive_path)
                dest = os.path.join(archive_path, f"BadData_{date}_{time}")
                if not os.path.exists(dest):
                    os.makedirs(dest)
                for file in os.listdir(source):
                    shutil.move(os.path.join(source, file), dest)
                self._log_message("Training_Logs/GeneralLog.txt", "Bad files moved to archive")
                shutil.rmtree(source)
                self._log_message("Training_Logs/GeneralLog.txt", "Bad Raw Data Folder Deleted successfully!!")
        except Exception as e:
            self._log_message("Training_Logs/GeneralLog.txt", f"Error while moving bad files to archive: {e}")
