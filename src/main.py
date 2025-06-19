import io
import os
import importlib
import pandas as pd
import pandera as pdr
import pyarrow
import yaml
from fastapi import FastAPI, File, Request, UploadFile, BackgroundTasks
from fastapi.templating import Jinja2Templates
from typing import Optional, Annotated
from sheetdrop.configuration import load_configurations, Configuration, MultipleSheetConfiguration
from sheetdrop.fileops import convert_file_to_dataframe, convert_file_to_dataframe_dict, clear_temp_dir, save_dataframe_to_cloud, save_table_to_cloud
from sheetdrop.fileops import store_temp_file, recover_temp_file, delete_temp_file
from sheetdrop.db import create_engine, save_file_status, load_latest_file_status

# import general configs from config.yaml
general_config = {}
try:
    with open("config.yaml") as f:
        general_config = yaml.safe_load(f)
except FileNotFoundError:
    print("ERROR: config.yaml not found. Please create a config.yaml file in the same directory as this file.")
    print("The file config.yaml.sample can be use as a template.")
    exit(1)

# call create_engine to get connection to utility database
engine = create_engine(general_config["database_url"])

#TODO: check if tables exist and are up to date


app = FastAPI()
templates = Jinja2Templates(directory="templates")


# Path to the directory where your configurations are stored
modules_dir = os.path.join(os.path.dirname(__file__), "file_definitions")

# Loop through all .py files in the file_definitions directory and import configurations into a dictionary
configurations, configuration_errors = load_configurations(modules_dir)


@app.get("/")
async def root(request: Request):
    """Endpoint to return a HTML page with a list of links to each file in your database."""
    return templates.TemplateResponse("index.html", {"files": configurations, "request": request})

@app.get("/file/{file_id}")
async def show_file(file_id: str, request: Request):
    """Endpoint to return a HTML page with a form to upload a file and current status."""
    status = load_latest_file_status(engine, file_id)
    return templates.TemplateResponse("file.html", {"file_id": file_id, "file_config": configurations[file_id], "status": status, "request": request})

@app.post("/file/{file_id}")
async def receive_file(file_id: str, file: UploadFile, request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint to receive a file and start a background task to validate it.
    file_id: str
        The id of the file to validate
    file: UploadFile
        The file to validate
    request: Request
        The request object
    background_tasks: BackgroundTasks
        The background tasks object
    Returns:
        A 202 Accepted response if the background task was successfully started.
        A 404 Not Found response if the file_id is not found in the configurations.
    """
    if(file_id not in configurations):
        return {"message": "File ID not found"}, 404
    save_file_status(engine, file_id, "in_progress")
    contents = io.BytesIO(file.file.read())
    file_path = store_temp_file(file_id, contents)
    background_tasks.add_task(process_file, file_id, file_path)
    if 'text/html' in request.headers.get('accept', ''):
        # Return Jinja template for browser requests
        return templates.TemplateResponse("redirect.html", {"file_id": file_id, "message": "Validation started in background", "request": request})
    else:
        # Return JSON response for API requests
        return {"message": "Validation started in background"}, 202
    
@app.get("/file/{file_id}/status")
async def get_file_status(file_id: str):
    """
    Endpoint to get the status of the validation of a file.
    file_id: str
        The id of the file for which to get the status
    Returns:
        The status of the validation of the file
    """
    status = load_latest_file_status(engine, file_id)
    return {"status": status}

async def process_file(file_id: str, file_path: str) -> None:
    """
    Validates and stores a file asynchronously.
    file_id: str
        The id of the file to validate
    file_path: str
        The temporary path of file to validate
    """
    file_conf = configurations[file_id]
    if isinstance(file_conf, MultipleSheetConfiguration):
        process_file_multiple_sheets(file_id, file_path, file_conf)
    else:
        dataframe = convert_file_to_dataframe(file_id, file_conf, file_path)
        schema = file_conf.schema
        try:
            pdr_schema = pdr.DataFrameSchema(schema, coerce=True)
            pdr_schema.validate(dataframe, lazy=True, inplace=True)
            save_file_status(engine, file_id, "saving")
            # save dataframe to appropriate location
            save_dataframe_to_cloud(dataframe, general_config["storage_provider"], file_conf.save_type, file_conf.save_location, file_conf.save_params)
            save_file_status(engine, file_id, "success")
        except (pyarrow.lib.ArrowInvalid, ValueError) as exc:
            save_file_status(engine, file_id, "failed" , str(exc))
        except pdr.errors.SchemaErrors as exc:
            save_file_status(engine, file_id, "failed" , str(exc.failure_cases).split("\n"))
        finally:
            delete_temp_file(file_path)


def process_file_multiple_sheets(file_id: str, file_path: str, file_conf: MultipleSheetConfiguration) -> None:
    """Validates and stores multiple sheets of a file asynchronously."""
    dataframe_dict = convert_file_to_dataframe_dict(file_id, configurations[file_id], file_path)
    errors = []
    partial_success = False
    for name, dataframe in dataframe_dict.items():
        schema = configurations[file_id].schema
        try:
            pdr_schema = pdr.DataFrameSchema(schema, coerce=True)
            pdr_schema.validate(dataframe, lazy=True, inplace=True)
            partial_success = True
            # save dataframe to appropriate location
            save_dataframe_to_cloud(dataframe, general_config["storage_provider"], file_conf.save_type, file_conf.save_location, file_conf.save_params)
        except pdr.errors.SchemaErrors as exc:
            errors.extend([f"{name}: {cause}" for cause in exc.failure_cases])
            break
    if errors and not partial_success:
        save_file_status(engine, file_id, "failed", errors)
    elif errors:
        save_file_status(engine, file_id, "partial_success", errors)
    else:
        save_file_status(engine, file_id, "success")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
