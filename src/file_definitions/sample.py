from pandera import Check, Column
from sheetdrop.configuration import Configuration

configuration = Configuration(
    name="Sample Excel",
    load_type="excel",
    load_params={},
    save_location="sample.parquet",
    save_type="parquet",
    save_params={},
    schema={
        "small_values": Column(float, [Check.less_than(100)]),
        "one_to_three": Column(int, [Check.isin([1, 2, 3])]),
        "phone_number": Column(str, [Check.str_matches(r'^[a-z0-9-]+$')]),
    }
)

