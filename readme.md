# Sheetdrop

A web application for validating, converting, and storing CSV and Excel files in a data lake.

## Configuration

### File definitions

The configuration for each file/table is loaded from the `file_definitions` directory. Each file in this directory should contain a single configuration class. This is an example of a valid configuration:
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
    schema=pa.DataFrameSchema({
        "small_values": pa.Column(float, pa.Check.less_than(100)),
        "one_to_three": pa.Column(int, pa.Check.isin([1, 2, 3])),
        "phone_number": pa.Column(str, pa.Check.str_matches(r'^[a-z0-9-]+$')),
    })
)
```

### General configuration file

The application won't run without a `config.yaml` configuration file in the root directory. The file should contain the following fields:

- `database_url`: URL used by SQLAlchemy to connect to the utility database
- `database_schema`: Schema where utility tables will be created
- `database_prefix`: Prefix for utility table names

## Running the application

To run the application locally, you can execute the following commands:
```bash	
cd src
uvicorn main:app --reload
```

For production deployment, you can use the Dockerfile to build an image and run it on the cloud.
