from fastapi import FastAPI, Path, Query, HTTPException, status, File, UploadFile, Request
from fastapi.encoders import jsonable_encoder
from typing import Optional, Annotated
#from pydantic import BaseModel
from Data_validation_and_classification_MA import data_file
from cyclic_temperature_dependent import TemperatureFileProcessor
import shutil
import os
#import io
from io import StringIO
import tempfile
#import mimetypes
import re
import logging
import datetime as dt
import magic
import json
from fastapi.responses import JSONResponse


def setup_logger(log_dir: str = "./Loggs") -> logging.Logger:
    """
    Setup a logger that creates a unique log file for each execution.

    :param log_dir: Directory to store log files.
    :return: Configured logger instance.
    """
    os.makedirs(log_dir, exist_ok=True)

    # Generate a unique log file name based on current date and time
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{timestamp}.log")

    # Set up the logger
    logger = logging.getLogger("execution_logger")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%y-%b-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)

    return logger


# Set up a single unique logger for this execution
logger = setup_logger()

app = FastAPI(docs_url="/")


@app.post(
    "/resistance/csv/data/tablebody",
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
    logger.info("Incoming request received")

    # Read file bytes from the request
    byte_data = await request.body()
    logger.info(f"File data read from request body successfully.")

    # Detect file type using python-magic
    file_type = magic.from_buffer(byte_data, mime=True)
    logger.info(f"Detected file type: {file_type}")

    # Map detected file type to appropriate extensions
    extension_map = {
        "text/csv": ".csv",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/json": ".json",
    }
    # Fallback to .csv if the type is unknown
    extension = extension_map.get(file_type, ".csv")
    logger.info(f"File extension determined as: {extension}")

    # Create a temporary file with the determined extension
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)

    try:
        # Write data to temp file and close it
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name
        logger.info(f"Temporary file created: {file_path}")

        # Use your validation logic on the saved file
        new_file = data_file(file_path)
        cyclic_temp_file = TemperatureFileProcessor(file_path)
        logger.info("Starting data validation and processing.")

        same_col_dict = new_file.find_type_and_keyword()
        logger.info(f"same_col_dict contents: {same_col_dict}")

        no_col = list(same_col_dict.keys())[1]
        logger.info(f"Column type detected: {no_col}")

        if no_col == 343:
            logger.info("Processing data as cyclic temperature file.")
            table = cyclic_temp_file.table_of_df_temp()
        else:
            logger.info("Processing data as general file.")
            table = new_file.table_of_df()

        logger.info("Data processing completed successfully.")

    finally:
        # Ensure temp file is deleted after processing
        os.unlink(file_path)
        logger.info(f"Temporary file deleted: {file_path}")

    return table


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = data_file(file_path)
    data_table = new_file.table_of_df()
    os.unlink(file_path)

    return data_table


@app.post(
    "/resistance/csv/data/databasevaluesbody",
    response_class=JSONResponse,
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
    logger.info("Incoming request received")
    try:
        byte_data = await request.body()
        file_size = len(byte_data)
        logger.info(f"File data read from request body successfully. File size: {file_size} bytes")

        # Detect file type using python-magic
        file_type = magic.from_buffer(byte_data, mime=True)
        logger.info(f"Detected file type: {file_type}")

        # Map detected file type to appropriate extensions
        extension_map = {
            "text/csv": ".csv",
            "application/vnd.ms-excel": ".xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "application/json": ".json",
        }
        extension = extension_map.get(file_type, ".csv")
        logger.info(f"File extension determined as: {extension}")

        # Create a temporary file with the determined extension
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)

        try:
            temp_file.write(byte_data)
            temp_file.close()
            file_path = temp_file.name
            logger.info(f"Temporary file created: {file_path}")

            # Use your validation logic on the saved file
            new_file = data_file(file_path)
            cyclic_temp_file = TemperatureFileProcessor(file_path)
            logger.info("Starting data validation and processing.")
            try:
                same_col_dict = new_file.find_type_and_keyword()
            except Exception as e:
                logger.error(f"Error during `find_type_and_keyword`: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to process the file.")
            logger.info(f"same_col_dict contents: {same_col_dict}")
            no_col = list(same_col_dict.keys())[1]
            logger.info(f"Column type detected: {no_col}")

            if no_col == 343:
                logger.info("Processing data as cyclic temperature file.")
                rMin_rMax_MA_values = cyclic_temp_file.info_R_in_MA_for_database_temp()
            else:
                logger.info("Processing data as general file.")
                rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()

            logger.info("Data processing completed successfully.")
        finally:
            os.unlink(file_path)
            logger.info(f"Temporary file deleted: {file_path}")

        logger.info(f"Response JSON: {json.dumps(rMin_rMax_MA_values, indent=4)}")
        # return rMin_rMax_MA_values
        # return json.dumps(rMin_rMax_MA_values, indent=4)
        # return rMin_rMax_MA_values



    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred during processing.")


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = data_file(file_path)
    rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
    os.unlink(file_path)

    return rMin_rMax_MA_values


@app.post(
    "/resistance/csv/overall/data/databasevaluesbody",
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
    # Read file bytes from the request
    byte_data = await request.body()

    # Detect file type using python-magic
    file_type = magic.from_buffer(byte_data, mime=True)

    # Map detected file type to appropriate extensions
    extension_map = {
        "text/csv": ".csv",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/json": ".json",
    }
    # Fallback to .csv if the type is unknown
    extension = extension_map.get(file_type, ".csv")

    # Create a temporary file with the determined extension
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)

    try:
        # Write data to temp file and close it
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name
        print("Temporary file saved as:", file_path)

        # Use your validation logic on the saved file
        new_file = data_file(file_path)
        cyclic_temp_file = TemperatureFileProcessor(file_path)
        same_col_dict = new_file.find_type_and_keyword()
        no_col = list(same_col_dict.keys())[1]
        if no_col == 343:
            rMin_rMax_MA_overall = cyclic_temp_file.info_for_database_temp()
        else:
            rMin_rMax_MA_overall = new_file.info_for_database()

    finally:
        # Ensure temp file is deleted after processing
        os.unlink(file_path)

    return rMin_rMax_MA_overall


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = data_file(file_path)
    rMin_rMax_temperature_values = new_file.info_for_database()
    os.unlink(file_path)

    return rMin_rMax_temperature_values


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = data_file(file_path)
    rMin_rMax_temperature_values = new_file.info_for_database()
    os.unlink(file_path)

    return rMin_rMax_temperature_values


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
    # Read file bytes from the request
    byte_data = await request.body()

    # Detect file type using python-magic
    file_type = magic.from_buffer(byte_data, mime=True)

    # Map detected file type to appropriate extensions
    extension_map = {
        "text/csv": ".csv",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/json": ".json",
    }
    # Fallback to .csv if the type is unknown
    extension = extension_map.get(file_type, ".csv")

    # Create a temporary file with the determined extension
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)

    try:
        # Write data to temp file and close it
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name
        print("Temporary file saved as:", file_path)

        # Use your validation logic on the saved file
        new_file = data_file(file_path)
        validation_status = new_file.file_validation()

    finally:
        # Ensure temp file is deleted after processing
        os.unlink(file_path)

    return validation_status


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = data_file(file_path)
    validation_status = new_file.file_validation()
    os.unlink(file_path)
    return validation_status


@app.post("/resistance/validation/file")
async def validation_of_incoming_file(file: UploadFile):
    file_path = file.filename

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    new_file = data_file(file_path)
    validation_status = new_file.file_validation()
    return validation_status

