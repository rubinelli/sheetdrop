# Sheetdrop

A web application for validating, converting, and storing CSV and Excel files in a data lake.

![Sample UI](docs/sample.png?raw=true)

## Configuration

### File definitions

The configuration for each file/table is loaded from the `file_definitions` directory. Each file in this directory should contain a single configuration object. This is an example of a valid configuration:
```python
import pandera as pa
from sheetdrop.configuration import Configuration

configuration = Configuration(
    name="Sample Excel",
    load_type="excel",
    load_params={},
    save_location="hdfs:///tables/sample_output",
    save_type="parquet",
    save_params={},
    schema={
        "small_values": pa.Column(float, pa.Check.less_than(100)),
        "one_to_three": pa.Column(int, pa.Check.isin([1, 2, 3])),
        "phone_number": pa.Column(str, pa.Check.str_matches(r'^[a-z0-9-]+$')),
    }
)
```
You can check the [Pandera documentation](https://pandera.readthedocs.io/en/stable/) for how to declare your schema and your validation rules.

When loading from CSV, we use Pandas's [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) function. 

Conversely, when loading from Excel, we use [pandas.read_excel](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html).

You can check the documentation to see what arguments you can pass to these functions through `load_params`.

#### Multiple sheets

This option is supported by the `MultipleSheetConfiguration` class and only available when loading from Excel.

When specifying multiple sheets, you shouldn't specify the `sheets` argument in `load_params`. Instead, these are collected from each `sheet` param in your list. This param can be the sheet's index (an int) or its name (a string).

### General configuration

The application can be configured using environment variables or a `config.yaml` file in the `src` directory. Environment variables take precedence.

The following variables are required:

- `DATABASE_URL`: URL used by SQLAlchemy to connect to the utility database
- `DATABASE_SCHEMA`: Schema where utility tables will be created (Optional. Must exist in the database; the application will not create it)
- `STORAGE_PROVIDER`: Where output data will be stored. Supported values: `s3`, `gcs`, `hdfs`, `local`

### Requirements

The application includes a `requirements.txt.sample` file. You can customize this file and save it as `requirements.txt` to include only the dependencies you need.

## Running the application

To run the application locally, you can execute the following commands:
```bash	
cd src
uvicorn main:app --reload
```

For production deployment, you can use the Dockerfile to build an image and run it on the cloud.
