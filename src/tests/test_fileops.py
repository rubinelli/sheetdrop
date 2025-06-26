import unittest
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import pyarrow
from sheetdrop import fileops

class TestFileops(unittest.TestCase):

    @patch('builtins.open')
    @patch('pandas.read_excel')
    def test_convert_file_to_dataframe_excel(self, mock_read_excel, mock_open):
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_excel.return_value = mock_df
        config = MagicMock()
        config.load_type = 'excel'
        config.load_params = {}

        df = fileops.convert_file_to_dataframe('test_file', config, 'dummy_path')

        self.assertTrue(df.equals(mock_df))
        mock_open.assert_called_with('dummy_path', 'rb')
        mock_read_excel.assert_called_once()

    @patch('builtins.open')
    @patch('pandas.read_csv')
    def test_convert_file_to_dataframe_csv(self, mock_read_csv, mock_open):
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_csv.return_value = mock_df
        config = MagicMock()
        config.load_type = 'csv'
        config.load_params = {}

        df = fileops.convert_file_to_dataframe('test_file', config, 'dummy_path')

        self.assertTrue(df.equals(mock_df))
        mock_open.assert_called_with('dummy_path', 'rb')
        mock_read_csv.assert_called_once()

    def test_convert_file_to_dataframe_invalid_type(self):
        config = MagicMock()
        config.load_type = 'invalid'
        with self.assertRaises(ValueError):
            fileops.convert_file_to_dataframe('test_file', config, 'dummy_path')

    @patch('os.path.exists')
    @patch('os.makedirs')
    @patch('builtins.open')
    def test_store_temp_file(self, mock_open, mock_makedirs, mock_exists):
        mock_exists.return_value = False
        file_content = b'test content'
        file_obj = fileops.io.BytesIO(file_content)

        path = fileops.store_temp_file('test_file', file_obj)

        mock_exists.assert_called_with('temp')
        mock_makedirs.assert_called_with('temp')
        self.assertTrue(path.startswith(os.path.join('temp', 'test_file_')))

    @patch('os.remove')
    def test_delete_temp_file(self, mock_remove):
        fileops.delete_temp_file('dummy_path')
        mock_remove.assert_called_with('dummy_path')

    @patch('pyarrow.parquet.write_table')
    @patch('pyarrow.fs.S3FileSystem')
    def test_save_table_to_cloud_s3_parquet(self, mock_s3_fs, mock_write_table):
        table = pyarrow.Table.from_pandas(pd.DataFrame())
        fileops.save_table_to_cloud(table, 's3', 'parquet', 's3://bucket/key')
        mock_s3_fs.assert_called_once()
        mock_write_table.assert_called_once()

    @patch('deltalake.write_deltalake')
    @patch('pyarrow.fs.GcsFileSystem')
    def test_save_table_to_cloud_gcs_deltalake(self, mock_gcs_fs, mock_write_deltalake):
        table = pyarrow.Table.from_pandas(pd.DataFrame())
        fileops.save_table_to_cloud(table, 'gcs', 'deltalake', 'gs://bucket/key')
        mock_gcs_fs.assert_called_once()
        mock_write_deltalake.assert_called_once()

    @patch('awswrangler.s3.to_parquet')
    def test_save_dataframe_to_cloud_s3_parquet(self, mock_to_parquet):
        df = pd.DataFrame()
        fileops.save_dataframe_to_cloud(df, 's3', 'parquet', 's3://bucket/key')
        mock_to_parquet.assert_called_once()

    @patch('deltalake.write_deltalake')
    def test_save_dataframe_to_cloud_local_deltalake(self, mock_write_deltalake):
        df = pd.DataFrame()
        fileops.save_dataframe_to_cloud(df, 'local', 'deltalake', '/path/to/table')
        mock_write_deltalake.assert_called_once()

if __name__ == '__main__':
    unittest.main()
