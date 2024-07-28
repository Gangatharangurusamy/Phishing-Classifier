from datetime import datetime


class App_Logger:
    """
    This class is used for logging purposes.

    Written By: iNeuron Intelligence
    Version: 2.0
    Revisions: Improved readability and performance
    """

    def __init__(self):
        pass

    def log(self, file_object, log_message):
        """
        Method Name: log
        Description: This method writes the log message to the specified file object.
        Input: file_object (file object), log_message (str)
        Output: None
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 2.0
        Revisions: Improved readability and performance
        """
        try:
            now = datetime.now()
            date = now.date()
            current_time = now.strftime("%H:%M:%S")
            log_entry = f"{date}/{current_time}\t\t{log_message}\n"
            file_object.write(log_entry)
        except Exception as e:
            raise Exception(f"Logging failed: {str(e)}")
