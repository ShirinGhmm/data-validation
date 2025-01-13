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
import pandas as pd
from fastapi.responses import JSONResponse
import filetype
import mimetypes
import csv



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
    "/resistance/data/tablebody",
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
async def Incoming_stream_processing_to_get_DataTable(file: UploadFile = File(...)):
    logger.info("Incoming request received")

    file_name = file.filename
    logger.info(f"File name received: {file_name}")

    # Read file bytes from the request
    byte_data = await file.read()
    logger.info(f"File data read from request body successfully.")
    # Map detected file type to appropriate extensions
    extension_map = {
        "text/csv": ".csv",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/json": ".json",
    }
    # Fallback to .csv if the type is unknown
    file_type = magic.from_buffer(byte_data, mime=True)
    extension = extension_map.get(file_type, ".csv")
    _, file_name_extension = os.path.splitext(file_name)

    if extension == ".csv" and file_name_extension == ".txt":
        extension = file_name_extension

    logger.info(f"File extension determined as: {extension}")


    if extension == ".txt":
        logger.info(f"I am in txt if")

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
        temp_file.write(byte_data)
        temp_file.close()
        file_path = temp_file.name

        new_file = data_file(file_path)
        data_table = new_file.table_of_df()
        os.unlink(file_path)

        return data_table

    else:
        logger.info(f"I am in csv else")
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
    "/resistance/data/databasevaluesbody",
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
async def Incoming_stream_processing_to_get_DataTable(file: UploadFile = File(...)):
    logger.info("Incoming request received")

    file_name = file.filename
    logger.info(f"File name received: {file_name}")

    try:
        # Read file bytes from the request
        byte_data = await file.read()
        logger.info("File data read from request body successfully.")

        # Map detected file type to appropriate extensions
        extension_map = {
            "text/csv": ".csv",
            "application/vnd.ms-excel": ".xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "application/json": ".json",
        }
        # Fallback to .csv if the type is unknown
        file_type = magic.from_buffer(byte_data, mime=True)
        extension = extension_map.get(file_type, ".csv")
        _, file_name_extension = os.path.splitext(file_name)

        if extension == ".csv" and file_name_extension == ".txt":
            extension = file_name_extension

        logger.info(f"File extension determined as: {extension}")

        if extension == ".txt":
            logger.info("Processing a .txt file.")
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
            try:
                temp_file.write(byte_data)
                temp_file.close()
                file_path = temp_file.name

                new_file = data_file(file_path)
                rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
            finally:
                os.unlink(file_path)
                logger.info(f"Temporary file deleted: {file_path}")

            return rMin_rMax_MA_values

        else:
            logger.info("Processing a .csv or other file format.")
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

            # Log the JSON response
            response_json = json.dumps(rMin_rMax_MA_values, indent=4)
            logger.info(f"Response JSON: {response_json}")

            return rMin_rMax_MA_values

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred during processing.")



@app.post(
    "/resistance/validation/body",
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
async def Incoming_stream_processing_to_get_DataTable(file: UploadFile = File(...)):
    logger.info("Incoming request received")

    try:
        # Extract file name and log
        file_name = file.filename
        logger.info(f"File name received: {file_name}")

        # Read file bytes from the request
        byte_data = await file.read()
        logger.info("File data read from request body successfully.")

        # Map detected file type to appropriate extensions
        extension_map = {
            "text/csv": ".csv",
            "application/vnd.ms-excel": ".xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
            "application/json": ".json",
        }

        # Determine file extension based on file type
        file_type = magic.from_buffer(byte_data, mime=True)
        extension = extension_map.get(file_type, ".csv")
        _, file_name_extension = os.path.splitext(file_name)

        # Adjust extension for specific cases
        if extension == ".csv" and file_name_extension == ".txt":
            extension = file_name_extension

        logger.info(f"File extension determined as: {extension}")

        # Handle text files
        if extension == ".txt":
            logger.info("Processing as a .txt file.")
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
            try:
                temp_file.write(byte_data)
                temp_file.close()
                file_path = temp_file.name

                # Perform validation
                new_file = data_file(file_path)
                validation_status = new_file.file_validation()
            finally:
                os.unlink(file_path)
                logger.info(f"Temporary file deleted: {file_path}")

            return validation_status

        # Handle CSV and other formats
        else:
            logger.info("Processing as a .csv or other file format.")
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=extension)
            try:
                temp_file.write(byte_data)
                temp_file.close()
                file_path = temp_file.name
                logger.info(f"Temporary file created: {file_path}")

                # Perform validation
                new_file = data_file(file_path)
                validation_status = new_file.file_validation()
            finally:
                os.unlink(file_path)
                logger.info(f"Temporary file deleted: {file_path}")

            return validation_status

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred during file validation.")

