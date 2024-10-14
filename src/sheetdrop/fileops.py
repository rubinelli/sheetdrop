
import io
import os
import importlib
import pandas as pd
import pyarrow
from pyarrow.orc import ORCWriter
from pyarrow.fs import HadoopFileSystem
from random import randint
from sheetdrop.configuration import load_configurations, Configuration, MultipleSheetConfiguration

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
    # add random sufix
    path = f"temp/{file_id}_{randint(0, 1000000)}"
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


def save_table_to_cloud(table: pyarrow.Table, provider: str, format: str, path: str, params: dict = {}):
    """
    Save a pyarrow Table to AWS S3, GCP GCS, HDFS, or local disk in the specified format.
    :param table: PyArrow Table to save.
    :param provider: String indicating the destination ('AWS', 'GCP', 'HDFS', 'LOCAL').
    :param format: String indicating the format to save ('orc', 'parquet', 'deltalake').
    :param path: The path to save the file (bucket/folder for cloud, HDFS path, or local file path).
    :param params: Additional parameters to pass to the saving function.
    """
    if provider not in ['AWS', 'GCP', 'HDFS', 'LOCAL']:
        raise ValueError("Provider must be 'AWS', 'GCP', 'HDFS', or 'LOCAL'")
    
    if format not in ['orc', 'parquet', 'deltalake']:
        raise ValueError("Format must be 'orc', 'parquet', or 'deltalake'")

    # Define the filesystem based on the provider
    if provider == 'AWS':
        # Create an S3 filesystem using pyarrow's S3FileSystem
        s3 = pyarrow.fs.S3FileSystem()
        filesystem = s3
    elif provider == 'GCP':
        # Create a GCS filesystem using pyarrow's GcsFileSystem
        gcs = pyarrow.fs.GcsFileSystem()
        filesystem = gcs
    elif provider == 'HDFS':
        # Create an HDFS filesystem
        hdfs = pyarrow.fs.HadoopFileSystem()
        filesystem = hdfs
    elif provider == 'LOCAL':
        # Use the local filesystem (default)
        filesystem = pyarrow.fs.LocalFileSystem()

    # Write to the appropriate format
    if format == 'orc':
        # Save as ORC
        with filesystem.open_output_stream(path) as f:
            pyarrow.orc.write_table(table, f, **params)
    elif format == 'parquet':
        # Save as Parquet
        pyarrow.parquet.write_table(table, path, filesystem=filesystem, **params)
    elif format == 'deltalake':
        # Deltalake currently does not have direct pyarrow support for cloud filesystems
        raise NotImplementedError("Deltalake is not supported for this implementation.")

def save_dataframe_to_cloud(df: pd.DataFrame, provider: str, format: str, path: str, params: dict = {}):
    """
    Save a pandas DataFrame to AWS S3, GCP GCS, HDFS, or local disk in the specified format.
    
    :param df: Pandas DataFrame to save.
    :param provider: String indicating the destination ('AWS', 'GCP', 'HDFS', 'LOCAL').
    :param format: String indicating the format to save ('orc', 'parquet', 'deltalake').
    :param path: The path to save the file (bucket/folder for cloud, HDFS path, or local file path).
    :param params: Additional parameters to pass to the saving function.
    """
    if provider not in ['AWS', 'GCP', 'HDFS', 'LOCAL']:
        raise ValueError("Provider must be 'AWS', 'GCP', 'HDFS', or 'LOCAL'")
    
    if format not in ['orc', 'parquet', 'deltalake']:
        raise ValueError("Format must be 'orc', 'parquet', or 'deltalake'")

    # Save to AWS S3
    if provider == 'AWS':
        import awswrangler as wr
        if format == 'orc':
            wr.s3.to_orc(df=df, path=path, **params)
        elif format == 'parquet':
            wr.s3.to_parquet(df=df, path=path, **params)
        elif format == 'deltalake':
            wr.s3.to_deltalake(df=df, path=path, **params)

    # Save to GCP GCS
    elif provider == 'GCP':
        import gcsfs
        fs = gcsfs.GCSFileSystem()
        if format == 'orc':
            with fs.open(path, 'wb') as f:
                df.to_orc(f, **params)
        elif format == 'parquet':
            df.to_parquet(path, engine='pyarrow', storage_options={"token": fs.credentials}, **params)
        elif format == 'deltalake':
            raise NotImplementedError("Deltalake is not supported for GCP in this implementation.")

    # Save to HDFS
    elif provider == 'HDFS':
        hdfs = HadoopFileSystem()
        if format == 'orc':
            with hdfs.open_output_stream(path) as f:
                orc_writer = ORCWriter(f)
                orc_writer.write_table(pa.Table.from_pandas(df))
        elif format == 'parquet':
            df.to_parquet(path, engine='pyarrow', filesystem=hdfs, **params)
        elif format == 'deltalake':
            raise NotImplementedError("Deltalake is not supported for HDFS in this implementation.")
    
    # Save to local disk
    elif provider == 'LOCAL':
        if format == 'orc':
            with open(path, 'wb') as f:
                orc_writer = ORCWriter(f)
                orc_writer.write_table(pa.Table.from_pandas(df))
        elif format == 'parquet':
            df.to_parquet(path, engine='pyarrow', **params)
        elif format == 'deltalake':
            import deltalake
            deltalake.write_deltalake(path, df, **params)

