
import io
import os
import importlib
import pandas as pd
import pandera as pa
from sheetdrop.configuration import load_configurations, Configuration, MultipleSheetConfiguration
from random import randint

# Basic I/O operations

def convert_file_to_dataframe(file_id: str, config: Configuration, file_path: str) -> pd.DataFrame:
    """
    Converts a file to a dataframe.
    file_id: str
        The id of the file to validate
    config: Configuration
        The configuration of the file to validate.
    file_path: str
        The path of the file to validate
    Returns:
        A dataframe
    """
    with open(file_path, "rb") as f:
        if(config.load_type == "excel"):
            dataframe = pd.read_excel(f, **config.load_params)
        elif(config.load_type == "csv"):
            dataframe = pd.read_csv(f, **config.load_params)
        elif(callable(config.load_type)):
            dataframe = config.load_type(f, **config.load_params)
        else:
            raise ValueError(f"Invalid load type for file {file_id}: {config.load_type}")
        return dataframe

def convert_file_to_dataframe_dict(file_id: str, config: MultipleSheetConfiguration, file_path: str) -> dict[str|int, pd.DataFrame]:
    """
    Converts a file to a dictionary of dataframes.
    file_id: str
        The id of the file to validate
    config: MultipleSheetConfiguration
        The configuration of the file to validate.
    file_path: str
        The path of the file to validate
    Returns:
        A dictionary of dataframes
    """
    load_params = config.load_params.copy() or {}
    load_params["sheets"] = config.sheets.keys()
    with open(file_path, "rb") as f:
        return pd.read_excel(f, **config.load_params)

def store_temp_file(file: io.BytesIO, file_id: str) -> str:
    """
    Stores a file in a temporary directory.
    file: io.BytesIO
        The contents of the file to store
    file_id: str
        The id of the file to store
    Returns:
        The path of the stored file
    """
    # add random sufix
    path = f"temp/{file_id}_{randint(0, 1000)}"
    if not os.path.exists("temp"):
        os.makedirs("temp")
    with open(path, "wb") as f:
        f.write(file.getbuffer())
    return path

def delete_temp_file(path):
    """
    Deletes a file from the temporary directory.
    path: str
        The path of the file to delete
    """
    os.remove(path)

def recover_temp_file(path: str) -> io.BytesIO:
    """
    Recovers a file from the temporary directory.
    path: str
        The path of the file to recover
    Returns:
        The contents of the file
    """
    with open(path, "rb") as f:
        return io.BytesIO(f.read())

def clear_temp_dir():
    """
    Clears the temporary directory.
    """
    if os.path.exists("temp"):
        for f in os.listdir("temp"):
            file_path = os.path.join("temp", f)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. {e}")
