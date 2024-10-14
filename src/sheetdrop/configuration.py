import importlib
import os
import pandera as pa
from dataclasses import dataclass
from typing import Any

def load_configurations(modules_dir):
    configurations = {}
    errors = []
    for filename in os.listdir(modules_dir):
        if filename.endswith(".py"):
            module_name = filename[:-3]  # Strip the .py extension
            module_path = f"file_definitions.{module_name}"

            # Dynamically import the module
            module = importlib.import_module(module_path)

            # Check if the module has an attribute called "configuration"
            if hasattr(module, "configuration"):
                #TODO: validate the configuration before adding to dictionary
                if not isinstance(module.configuration, Configuration) and not isinstance(module.configuration, MultipleSheetConfiguration):
                    errors.append(f"Invalid configuration for {module_name}: {module.configuration}")
                configurations[module_name] = module.configuration
                print(f"Loaded configuration: {module_name}")
            else:
                errors.append(f"Module {module_name} does not have a configuration attribute")
    return configurations, errors

@dataclass
class Configuration():
    name: str
    save_location: str
    schema: dict[str, pa.Column]
    load_type: str = "excel"
    load_params: dict[str, Any] = None
    save_type: str = "parquet"
    save_params: dict[str, Any] = None
    
@dataclass
class SheetConfiguration():
    sheet: str | int
    save_location: str
    schema: dict[str, pa.Column]
    save_type: str = "parquet"
    save_params: dict[str, Any] = None
    
@dataclass
class MultipleSheetConfiguration():
    name: str
    sheets: list[SheetConfiguration]
    load_params: dict[str, Any] = None
