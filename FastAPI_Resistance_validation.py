from fastapi import FastAPI, Path, Query, HTTPException, status, File, UploadFile, Request
from fastapi.encoders import jsonable_encoder
from typing import Optional, Annotated
from pydantic import BaseModel
from Data_validation_and_classification_MA import Data_File
import shutil
import os
# import io
from io import StringIO
import tempfile
import mimetypes
import re
import logging
import datetime as dt

# Ensure the log directory exists
log_directory = './Loggs'
os.makedirs(log_directory, exist_ok=True)

# Set up logging
today = dt.datetime.today()
file_name = f"./Loggs/{today.year}-{today.month:02}-{today.day:02}.log"

logging.basicConfig(filename=file_name, level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%y-%b-%d %H:%M:%S')

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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = Data_File(file_path)
    table = new_file.table_of_df()
    os.unlink(file_path)
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

    new_file = Data_File(file_path)
    data_table = new_file.table_of_df()
    os.unlink(file_path)

    return data_table


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = Data_File(file_path)
    rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
    os.unlink(file_path)

    return rMin_rMax_MA_values


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

    new_file = Data_File(file_path)
    rMin_rMax_MA_values = new_file.info_R_in_MA_for_database()
    os.unlink(file_path)

    return rMin_rMax_MA_values


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
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name

    new_file = Data_File(file_path)
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

    new_file = Data_File(file_path)
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
    # data: bytes = await request.body()
    byte_data = await request.body()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.write(byte_data)
    temp_file.close()
    file_path = temp_file.name
    print(file_path)
    new_file = Data_File(file_path)
    validation_status = new_file.file_validation()
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

    new_file = Data_File(file_path)
    validation_status = new_file.file_validation()
    os.unlink(file_path)
    return validation_status


@app.post("/resistance/validation/file")
async def validation_of_incoming_file(file: UploadFile):
    file_path = file.filename

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    new_file = Data_File(file_path)
    validation_status = new_file.file_validation()
    return validation_status


if __name__ == "__main__":
    logging.getLogger().handlers[0].flush()
