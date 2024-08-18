from fastapi import FastAPI, Request, UploadFile
# provides methods for handling and validating data files
from Data_validation_and_classification_MA import Data_File
# copying, moving files,removing directories and their contents.
import shutil
# reading and writing to the file system
import os
# creating temporary files and directories
import tempfile
# record events, errors, and other information during the execution.
import logging
# handle dates and times
import datetime as dt
# extracting and debugging, help to understand the flow of the program and locate where errors occur.
import traceback

# specifying the path where log files will be stored. "./" prefix means "current directory"
log_directory = './Logs'
# Ensure the log directory exists.
os.makedirs(log_directory, exist_ok=True)


# creating a logger for each request:
def create_logger():
    """
    This function is designed to create a logger instance with a unique name based on the current timestamp. This
    logger writes log messages to a file in a specified log directory. It gets the current date and time and
    constructs the log file path by combining the log directory path and the timestamp.
    It sets the logging level to capture all messages.
    file handler that writes log messages to the constructed log file path is created.
    Finally, it adds the handler to the logger, directing any log messages sent to the logger to be written to the file.
    The function returns the configured logger instance, ready to log messages for the specific request.
    """
    # timestamp is a numeric representation of time.it gets the current date and time and formats the date and time
    # as a string.
    timestamp = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    # Constructing the Log File Path by combining the log directory and the timestamp.
    log_file_path = f"{log_directory}/{timestamp}.log"
    """By using timestamp as the name, each call to create_logger generates a logger with a unique name based on the 
    current date and time."""
    # Creates a new logger instance named after the timestamp. This ensures that each logger is unique.
    # Loggers expose the interface that application code directly uses.
    logger = logging.getLogger(timestamp)
    # the logger will capture all messages at the INFO level and above.
    logger.setLevel(logging.INFO)
    # defining format of the log messages including the timestamp and the log message.
    # Formatter specify the layout of log records in the final output.
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # writing log messages to the file.
    # Handlers send the log message (created by loggers) to the specified log file.
    file_handler = logging.FileHandler(log_file_path)
    # ensures that each log message is formatted according to the formatter defined earlier.
    file_handler.setFormatter(formatter)
    # Adds the file handler to the logger, so any log messages sent to the logger will be written to the file.
    logger.addHandler(file_handler)

    return logger


"""
Creating an Instance of FastAPI:
a new FastAPI application instance will be created.
And the URL path for accessing the automatically generated API documentation
"""
app = FastAPI(docs_url="/")

# POST: It means the user wants to send data to the API (to create something, or store something in the database).
"""it defines an endpoint responding to HTTP POST requests.
it processes incoming data including handling temporary file creation, logging, and error handling.
"""


@app.post(
    "/resistance/csv/data/tablebody",
    openapi_extra={
        # request body is in application/octet-stream format:binary files
        "requestBody": {
            # content type and structure of the request body
            "content": {
                # binary files format
                "application/octet-stream": {
                    "schema": {
                        # schema of the data is an array
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
    Handles incoming CSV data requests, processes the data, and returns a DataTable. It enables the program to manage
    other tasks concurrently while waiting for operations such as reading a request body to complete. The function
    reads and processes the request body, which contains the data sent by the user. A logger instance is created to
    log messages specific to this request. it reads incoming request and store it in byte_data as raw bytes. It then
    creates a temporary file to store the uploaded data. This file is created with a .csv suffix and is not
    automatically deleted when closed. The raw data from the request body is written to this temporary file.
    temporary file path is stored in the file_path variable. an instance of Data_File is created using the temporary
    file path. table_of_df method is called to process the file. If an exception occurs, it is caught and assigned to
    the variable e. full traceback of the exception is logged to provide information about where the error occurred.
    Finally, regardless of whether an exception was raised, if temporary file was successfully created, the temporary
    file will be deleted from the filesystem.
    """
    # Variable Initialization
    # storing the path of the temporary file created to hold the uploaded data
    file_path = None
    # get a logger instance which will be used to log messages specific to this request
    logger = create_logger()
    # Processing the Incoming Request:
    # handle potential exceptions that might occur during the execution
    try:
        # reads the entire body of the incoming request and stores it in byte_data as raw bytes
        byte_data = await request.body()

        # Temporary File Creation and Logging:
        # create a temporary file that will not be automatically deleted (delete=False) when closed.
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        # writes the received byte_data (the raw data from the request body) to the temporary file
        temp_file.write(byte_data)
        # closes the temporary file, ensuring that all data is written and the file is properly saved.
        temp_file.close()
        # stores the path of the temporary file
        file_path = temp_file.name

        # Logs an informational message indicating that a CSV data table request was received
        logger.info("Received CSV data table request.")
        # Logs the path to the temporary file that was created
        logger.info(f"Temporary file created: {file_path}")

        # Creates an instance of Data_File with the temporary file path.
        new_file = Data_File(file_path)
        # Calls the table_of_df method of the Data_File instance returning table or DataFrame.
        table = new_file.table_of_df()
        # deletes the temporary file from the filesystem
        os.unlink(file_path)
        logger.info(f"Temporary CSV file deleted at {file_path}")
        return table

    # Exception Handling
    # Catches any exception that occurs within the try block and assigns the exception object to the variable e
    except Exception as e:
        # Sets error_file_path to a default message.
        error_file_path = "Temporary file not created"
        # Logs an error message indicating that an error occurred
        logger.error(f"Error processing CSV data table: {e}")

        """ # Logs the full traceback of the exception, providing information about where the error occurred.
        It provides information about the error type, error message, and the sequence of function calls that led 
        to the error."""
        logger.error(traceback.format_exc())  # format_exc(): Formatting the Exception
        # Returns a JSON response containing the error message and the path to the problematic file.
        return {"error": str(e), "problematic_file": error_file_path}


"""it defines an endpoint responding to HTTP POST requests.
it processes incoming data including handling temporary file creation, logging, and error handling.
"""


@app.post(
    "/resistance/txt/data/tablebody",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
        Handles incoming TXT data requests, processes the data, and returns a DataTable.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received TXT data table request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        data_table = new_file.table_of_df()
        os.unlink(file_path)
        logger.info(f"Temporary TXT file deleted at {file_path}")
        return data_table

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error processing TXT data table: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/csv/data/databasevaluesbody",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
        Handles incoming CSV database values requests, processes the data, and returns rMin_rMax_MA values.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received CSV database values request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
        os.unlink(file_path)
        logger.info(f"Temporary CSV file deleted at {file_path}")

        return rMin_rMax_MA_values

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error processing CSV database values: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/txt/data/databasevaluesbody",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
       Handles incoming TXT database values requests, processes the data, and returns rMin_rMax_MA values.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received TXT database values request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
        os.unlink(file_path)
        logger.info(f"Temporary TXT file deleted at {file_path}")
        return rMin_rMax_MA_values

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error processing TXT database values: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/csv/Overall/data/databasevaluesbody",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
        Handles incoming CSV overall database values requests, processes the data, and returns rMin_rMax_temperature values.
    """
    file_path = None
    logger = create_logger()
    try:

        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received CSV overall database values request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        rMin_rMax_temperature_values = new_file.info_for_database()
        os.unlink(file_path)
        logger.info(f"Temporary CSV file deleted at {file_path}")
        return rMin_rMax_temperature_values

    except Exception as e:
        error_file_path = "Temporary file not created"

        logger.error(f"Error processing CSV overall database values: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/txt/Overall/data/databasevaluesbody",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def Incoming_stream_processing_to_get_DataTable(request: Request):
    """
        Handles incoming TXT overall database values requests, processes the data, and returns rMin_rMax_temperature values.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received TXT overall database values request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        rMin_rMax_temperature_values = new_file.info_for_database()
        os.unlink(file_path)
        logger.info(f"Temporary TXT file deleted at {file_path}")
        return rMin_rMax_temperature_values

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error processing TXT overall database values: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/csv/validation/body",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def validation_of_incoming_file(request: Request):
    """
        Handles incoming CSV file validation requests, processes the file, and returns validation status.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received CSV validation request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        validation_status = new_file.file_validation()
        os.unlink(file_path)
        logger.info(f"Temporary CSV file deleted at {file_path}")
        return validation_status

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error during CSV validation: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post(
    "/resistance/txt/validation/body",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/octet-stream": {
                    "schema": {
                        "type": "array",
                    }
                }
            }
        }
    },
)
async def validation_of_incoming_file(request: Request):
    """
        Handles incoming TXT file validation requests, processes the file, and returns validation status.
    """
    file_path = None
    logger = create_logger()
    try:
        byte_data = await request.body()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        logger.info("Received TXT validation request.")
        logger.info(f"Temporary file created: {file_path}")

        new_file = Data_File(file_path)
        validation_status = new_file.file_validation()
        os.unlink(file_path)
        logger.info(f"Temporary TXT file deleted at {file_path}")
        return validation_status

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error during TXT validation: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


@app.post("/resistance/validation/file")
# function accepts a file as an argument. UploadFile is a class for handling file uploads.
async def validation_of_incoming_file(file: UploadFile):
    """
        Handles file upload validation requests, processes the uploaded file, and returns validation status.
    """
    # This will later hold the path of the uploaded file.
    file_path = None
    # Initializes a logger for logging messages related to this request.
    logger = create_logger()
    # Processing the Uploaded File
    try:
        # Assigns the name of the uploaded file to file_path
        file_path = file.filename
        # a log file path is constructed for the uploaded file.
        log_file_path = f"{log_directory}/{file.filename}.log"
        # reinitializes the logger to log to the uploaded file for this request.
        logger = create_logger(log_file_path)
        # Logs an informative message indicating that a file validation request has been received by server
        logger.info(f"Received file validation request for: {file.filename}")
        # Saving the Uploaded File: opens a new file on the server with the same name as the uploaded file for
        # writing in binary mode ("wb": write binary). The with statement ensures that the file is properly closed
        # after writing.
        with open(file_path, "wb") as buffer:
            # Copies the contents of the uploaded file (file.file) to the newly opened file (buffer)
            # file.file is a file-like object representing the uploaded file
            # buffer is the file object opened on the server
            shutil.copyfileobj(file.file, buffer)
        # Logs a message indicating that the uploaded file has been saved to a temporary location on the server.
        logger.info(f"Temporary file saved: {file_path}")

        # File Validation
        # Creates an instance of Data_File with the path of the saved file.
        new_file = Data_File(file_path)
        # Calls a method file_validation on the Data_File instance to validate the file.
        validation_status = new_file.file_validation()
        # Logs the result of the file validation.
        logger.info(f"Validation result for {file.filename}: {validation_status}")
        # Returns the validation status as the response.
        os.unlink(file_path)

        return validation_status

    except Exception as e:
        error_file_path = "Temporary file not created"
        logger.error(f"Error during file validation: {e}")
        logger.error(traceback.format_exc())
        return {"error": str(e), "problematic_file": error_file_path}


# Run the FastAPI application:

# checks if the current module is being run as the main program
if __name__ == "__main__":
    # This line retrieves the root logger instance in Python's logging module
    logger = logging.getLogger()
    # checks if the root logger has any handlers attached.
    # it means there are one or more handlers attached to the root logger.
    if logger.handlers:
        # a loop over each handler attached to the root logger.
        for handler in logger.handlers:
            # ensures that all logging output has been written to the respective destination(a file).
            # ensures that all log messages have been saved to disk and nothing is left in a buffer.
            handler.flush()
