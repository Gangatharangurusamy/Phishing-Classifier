import shutil
import sqlite3
import os
import csv
from os import listdir
from application_logging.logger import App_Logger


class dBOperation:
    """
    This class handles all SQL operations for training data.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None
    """

    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self, DatabaseName):
        """
        Establishes a connection to the database or creates a new one if it doesn't exist.

        Parameters:
        DatabaseName (str): The name of the database.

        Returns:
        sqlite3.Connection: Connection object to the database.

        Raises:
        ConnectionError: If connection to the database fails.
        """
        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Opened {DatabaseName} database successfully")
            file.close()
            return conn
        except ConnectionError as e:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Error while connecting to database: {str(e)}")
            file.close()
            raise ConnectionError(f"Error while connecting to database: {str(e)}")

    def createTableDb(self, DatabaseName, column_names):
        """
        Creates a table in the database with given columns.

        Parameters:
        DatabaseName (str): The name of the database.
        column_names (dict): Dictionary containing column names as keys and their data types as values.

        Raises:
        Exception: If table creation fails.
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            cursor = conn.cursor()
            cursor.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")

            if cursor.fetchone()[0] == 1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Table 'Good_Raw_Data' already exists in database.")
                file.close()
            else:
                for key, type_ in column_names.items():
                    try:
                        conn.execute(f'ALTER TABLE Good_Raw_Data ADD COLUMN "{key}" {type_}')
                    except:
                        conn.execute(f'CREATE TABLE Good_Raw_Data ("{key}" {type_})')

                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Table 'Good_Raw_Data' created successfully in database.")
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, f"Error while creating table: {str(e)}")
            file.close()
            conn.close()
            raise e

    def insertIntoTableGoodData(self, Database):
        """
        Inserts data from CSV files in 'Good_Raw' folder into 'Good_Raw_Data' table in the database.

        Parameters:
        Database (str): The name of the database.

        Raises:
        Exception: If data insertion fails.
        """
        conn = self.dataBaseConnection(Database)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        try:
            for file in onlyfiles:
                with open(os.path.join(goodFilePath, file), "r") as f:
                    next(f)  # Skip header
                    reader = csv.reader(f, delimiter="\n")
                    for line in reader:
                        try:
                            conn.execute(f'INSERT INTO Good_Raw_Data values ({line[0]})')
                            self.logger.log(log_file, f"{file}: File loaded successfully!")
                            conn.commit()
                        except Exception as e:
                            raise e

        except Exception as e:
            conn.rollback()
            self.logger.log(log_file, f"Error while inserting into table: {str(e)}")
            shutil.move(os.path.join(goodFilePath, file), badFilePath)
            self.logger.log(log_file, f"File Moved Successfully: {file}")

        finally:
            conn.close()
            log_file.close()

    def selectingDatafromtableintocsv(self, Database):
        """
        Exports data from 'Good_Raw_Data' table in the database to a CSV file.

        Parameters:
        Database (str): The name of the database.

        Raises:
        Exception: If data export fails.
        """
        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')

        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()

            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            # Make the CSV output directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing
            with open(os.path.join(self.fileFromDb, self.fileName), 'w', newline='') as csvFile:
                csv_writer = csv.writer(csvFile, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL,
                                        escapechar='\\')
                csv_writer.writerow(headers)
                csv_writer.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, f"File exporting failed. Error: {str(e)}")
            raise e

        finally:
            conn.close()
            log_file.close()
