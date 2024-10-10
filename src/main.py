import io
import os
import importlib
import pandas as pd
import pandera as pa
import yaml
from fastapi import FastAPI, File, Request, UploadFile, BackgroundTasks
from fastapi.templating import Jinja2Templates
from typing import Optional, Annotated
from sheetdrop.configuration import load_configurations
from sheetdrop.fileops import convert_file_to_dataframe, clear_temp_dir
from sheetdrop.fileops import store_temp_file, recover_temp_file, delete_temp_file
from sheetdrop.db import create_engine, create_tables

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

# create tables if they don't exist
#create_tables(engine, general_config["database_schema"], general_config["database_table_prefix"])

app = FastAPI()
templates = Jinja2Templates(directory="templates")


# Path to the directory where your configurations are stored
modules_dir = os.path.join(os.path.dirname(__file__), "file_definitions")

# Loop through all .py files in the file_definitions directory and import configurations into a dictionary
configurations, configuration_errors = load_configurations(modules_dir)

# Keep status in a dict for now
status = {}

@app.get("/")
async def root(request: Request):
    """Endpoint to return a HTML page with a list of links to each file in your database."""
    return templates.TemplateResponse("index.html", {"files": configurations, "request": request})

@app.get("/file/{file_id}")
async def show_file(file_id: str, request: Request):
    """Endpoint to return a HTML page with a form to upload a file."""
    return templates.TemplateResponse("file.html", {"files": configurations, "request": request})

@app.post("/file/{file_id}")
async def receive_file(file_id: str, file: UploadFile, background_tasks: BackgroundTasks):
    """
    Endpoint to receive a file and start a background task to validate it.
    file_id: str
        The id of the file to validate
    file: UploadFile
        The file to validate
    Returns:
        A 202 Accepted response if the background task was successfully started.
        A 404 Not Found response if the file_id is not found in the configurations.
    """
    if(file_id not in configurations):
        return {"message": "File ID not found"}, 404
    status[file_id] = "in_progress"
    contents = io.BytesIO(file.file.read())
    file_path = store_temp_file(file_id, contents)
    background_tasks.add_task(process_file, file_id, file_path)
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
    # TODO: Implement a system to store the status of validations
    # For now, always return "in_progress"
    return {"status": status[file_id]}

async def process_file(file_id: str, file_path: str):
    """
    Validates and stores a file asynchronously.
    file_id: str
        The id of the file to validate
    file_path: str
        The temporary path of file to validate
    """
    if isinstance(configurations[file_id], MultipleSheetConfiguration):
        process_file_multiple_sheets(file_id, file_path)
    dataframe = convert_file_to_dataframe(file_id, file_path)
    schema = configurations[file_id].schema
    try:
        schema.validate(dataframe, lazy=True)
        status[file_id] = "success"
    except pa.errors.SchemaErrors as exc:
        status[file_id] = "failed\n" + str(exc.failure_cases)


def process_file_multiple_sheets(file_id: str, file_path: str):
    """Validates and stores multiple sheets of a file asynchronously."""
    dataframe_dict = convert_file_to_dataframe_dict(file_id, configurations[file_id], file_path)
    errors = []
    for name, dataframe in dataframe_dict.items():
        schema = configurations[file_id].schema
        try:
            schema.validate(dataframe, lazy=True)
        except pa.errors.SchemaErrors as exc:
            errors.append(f"{name}:\n" + str(exc.failure_cases))
            break
    if errors:
        status[file_id] = "failed\n" + "\n".join(errors)
    else:
        status[file_id] = "success"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
