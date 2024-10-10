import pandera as pa
from sheetdrop.configuration import Configuration

configuration = Configuration(
    name="Sample Excel",
    load_type="excel",
    load_params={},
    save_location="",
    save_type="parquet",
    save_params={},
    schema=pa.DataFrameSchema({
        "small_values": pa.Column(float, pa.Check.less_than(100)),
        "one_to_three": pa.Column(int, pa.Check.isin([1, 2, 3])),
        "phone_number": pa.Column(str, pa.Check.str_matches(r'^[a-z0-9-]+$')),
    })
)
