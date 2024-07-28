import shutil
import sqlite3
import os
import csv
from os import listdir
from application_logging.logger import App_Logger


class dBOperation:
    """
    This class shall be used for handling all the SQL operations.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None
    """

    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self, DatabaseName):
        """
        Method Name: dataBaseConnection
        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
        Output: Connection to the DB
        On Failure: Raise ConnectionError

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError as e:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, f"Error while connecting to database: {str(e)}")
            file.close()
            raise e
        return conn

    def createTableDb(self, DatabaseName, column_names):
        """
        Method Name: createTableDb
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key, type in column_names.items():
                try:
                    conn.execute(
                        'ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,
                                                                                                 dataType=type))
                except:
                    conn.execute(
                        'CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, f"Error while creating table: {str(e)}")
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e

    def insertIntoTableGoodData(self, Database):
        """
        Method Name: insertIntoTableGoodData
        Description: This method inserts the Good data files from the Good_Raw folder into the above created table.
        Output: None
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        conn = self.dataBaseConnection(Database)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')

        try:
            for file in onlyfiles:
                with open(goodFilePath + '/' + file, "r") as f:
                    next(f)  # Skip header
                    reader = csv.reader(f, delimiter="\n")
                    for line in reader:
                        try:
                            conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=line[0]))
                            self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                            conn.commit()
                        except Exception as e:
                            raise e

        except Exception as e:
            conn.rollback()
            self.logger.log(log_file, f"Error while inserting into table: {str(e)}")
            shutil.move(goodFilePath + '/' + file, badFilePath)
            self.logger.log(log_file, f"File Moved Successfully {file}")

        finally:
            conn.close()
            log_file.close()

    def selectingDatafromtableintocsv(self, Database):
        """
        Method Name: selectingDatafromtableintocsv
        Description: This method exports the data in GoodData table as a CSV file in a given location.
        Output: None
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
        """
        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')

        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)
            results = cursor.fetchall()

            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            # Make the CSV output directory if it doesn't exist
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''), delimiter=',',
                                 lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, f"File exporting failed. Error: {str(e)}")
            raise e

        finally:
            conn.close()
            log_file.close()
