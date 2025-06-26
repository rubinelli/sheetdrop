import io
import os
import pandas as pd
import pyarrow
from pyarrow.fs import HadoopFileSystem
from random import randint
from sheetdrop.configuration import Configuration, MultipleSheetConfiguration

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
    readers = {
        "excel": pd.read_excel,
        "csv": pd.read_csv,
    }
    reader = readers.get(config.load_type)
    if not reader and not callable(config.load_type):
        raise ValueError(f"Invalid load type for file {file_id}: {config.load_type}")

    with open(file_path, "rb") as f:
        if reader:
            return reader(f, **config.load_params)
        elif callable(config.load_type):
            return config.load_type(f, **config.load_params)

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
    load_params = config.load_params.copy() if config.load_params else {}
    load_params["sheet_name"] = list(config.sheets.keys())
    with open(file_path, "rb") as f:
        return pd.read_excel(f, **load_params)

def store_temp_file(file_id: str, file: io.BytesIO) -> str:
    """
    Stores a file in a temporary directory.
    file_id: str
        The id of the file to store
    file: io.BytesIO
        The contents of the file to store
    Returns:
        The path of the stored file
    """
    temp_dir = "temp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    path = os.path.join(temp_dir, f"{file_id}_{randint(0, 1000000)}")
    with open(path, "wb") as f:
        f.write(file.getbuffer())
    return path

def delete_temp_file(path):
    """
    Deletes a file from the temporary directory.
    path: str
        The path of the file to delete
    """
    try:
        os.remove(path)
    except OSError as e:
        print(f"Error deleting file {path}: {e}")

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
    temp_dir = "temp"
    if os.path.exists(temp_dir):
        for f in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, f)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. {e}")

def save_table_to_cloud(table: pyarrow.Table, provider: str, format: str, path: str, params: dict = None):
    """
    Save a pyarrow Table to AWS S3, GCP GCS, HDFS, or local disk in the specified format.
    :param table: PyArrow Table to save.
    :param provider: String indicating the destination ('s3', 'gcs', 'hdfs', 'local').
    :param format: String indicating the format to save ('parquet', 'deltalake').
    :param path: The path to save the file (bucket/folder for cloud, HDFS path, or local file path).
    :param params: Additional parameters to pass to the saving function.
    """
    params = params or {}
    filesystems = {
        "s3": pyarrow.fs.S3FileSystem,
        "gcs": pyarrow.fs.GcsFileSystem,
        "hdfs": pyarrow.fs.HadoopFileSystem,
        "local": pyarrow.fs.LocalFileSystem,
    }
    fs_class = filesystems.get(provider)
    if not fs_class:
        raise ValueError(f"Provider must be one of {list(filesystems.keys())}")

    def deltalake_writer(table, path, **kwargs):
        from deltalake import write_deltalake
        storage_options = kwargs.pop("storage_options", None)
        write_deltalake(path, table, storage_options=storage_options, **kwargs)

    writers = {
        "parquet": pyarrow.parquet.write_table,
        "deltalake": deltalake_writer,
    }
    writer = writers.get(format)
    if not writer:
        raise ValueError(f"Format must be one of {list(writers.keys())}")

    writer(table, path, filesystem=fs_class(), **params)

def save_dataframe_to_cloud(df: pd.DataFrame, provider: str, format: str, path: str, params: dict = None):
    """
    Save a pandas DataFrame to AWS S3, GCP GCS, HDFS, or local disk in the specified format.
    
    :param df: Pandas DataFrame to save.
    :param provider: String indicating the destination ('s3', 'gcs', 'hdfs', 'local').
    :param format: String indicating the format to save ('parquet', 'deltalake').
    :param path: The path to save the file (bucket/folder for cloud, HDFS path, or local file path).
    :param params: Additional parameters to pass to the saving function.
    """
    params = params or {}
    
    def s3_parquet_saver(df, path, **kwargs):
        import awswrangler as wr
        wr.s3.to_parquet(df=df, path=path, **kwargs)

    def gcs_parquet_saver(df, path, **kwargs):
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        df.to_parquet(path, engine='pyarrow', storage_options={"token": fs.credentials}, **kwargs)

    def hdfs_parquet_saver(df, path, **kwargs):
        fs = HadoopFileSystem()
        df.to_parquet(path, engine='pyarrow', filesystem=fs, **kwargs)
        
    def local_parquet_saver(df, path, **kwargs):
        df.to_parquet(path, engine='pyarrow', **kwargs)

    def deltalake_saver(df, path, **kwargs):
        from deltalake import write_deltalake
        storage_options = kwargs.pop("storage_options", None)
        write_deltalake(path, df, mode="overwrite", storage_options=storage_options, **kwargs)

    savers = {
        "s3": {
            "parquet": s3_parquet_saver,
            "deltalake": deltalake_saver,
        },
        "gcs": {
            "parquet": gcs_parquet_saver,
            "deltalake": deltalake_saver,
        },
        "hdfs": {
            "parquet": hdfs_parquet_saver,
            "deltalake": deltalake_saver,
        },
        "local": {
            "parquet": local_parquet_saver,
            "deltalake": deltalake_saver,
        },
    }

    saver = savers.get(provider, {}).get(format)
    if not saver:
        raise ValueError(f"Invalid provider or format: {provider}, {format}")

    saver(df, path, **params)