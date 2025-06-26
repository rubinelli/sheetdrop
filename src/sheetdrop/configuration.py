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
                config = module.configuration
                if isinstance(config, (Configuration, MultipleSheetConfiguration)):
                    validation_errors = config.validate()
                    if validation_errors:
                        errors.append(f"Invalid configuration for {module_name}: {', '.join(validation_errors)}")
                    else:
                        configurations[module_name] = config
                        print(f"Loaded configuration: {module_name}")
                else:
                    errors.append(f"Invalid configuration for {module_name}: {config}")
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

    def validate(self) -> list[str]:
        errors = []
        if not isinstance(self.name, str) or not self.name:
            errors.append("Configuration.name must be a non-empty string")
        if not isinstance(self.save_location, str) or not self.save_location:
            errors.append("Configuration.save_location must be a non-empty string")
        if not isinstance(self.schema, dict):
            errors.append("Configuration.schema must be a dictionary")
        if self.load_params and not isinstance(self.load_params, dict):
            errors.append("Configuration.load_params must be a dictionary")
        if self.save_params and not isinstance(self.save_params, dict):
            errors.append("Configuration.save_params must be a dictionary")
        return errors

@dataclass
class SheetConfiguration():
    sheet: str | int
    save_location: str
    schema: dict[str, pa.Column]
    save_type: str = "parquet"
    save_params: dict[str, Any] = None

    def validate(self) -> list[str]:
        errors = []
        if not isinstance(self.sheet, (str, int)) or (isinstance(self.sheet, str) and not self.sheet):
            errors.append("SheetConfiguration.sheet must be a non-empty string or an integer")
        if not isinstance(self.save_location, str) or not self.save_location:
            errors.append("SheetConfiguration.save_location must be a non-empty string")
        if not isinstance(self.schema, dict):
            errors.append("SheetConfiguration.schema must be a dictionary")
        if self.save_params and not isinstance(self.save_params, dict):
            errors.append("SheetConfiguration.save_params must be a dictionary")
        return errors
    
@dataclass
class MultipleSheetConfiguration():
    name: str
    sheets: list[SheetConfiguration]
    load_params: dict[str, Any] = None

    def validate(self) -> list[str]:
        errors = []
        if not isinstance(self.name, str) or not self.name:
            errors.append("MultipleSheetConfiguration.name must be a non-empty string")
        if not isinstance(self.sheets, list) or not self.sheets:
            errors.append("MultipleSheetConfiguration.sheets must be a non-empty list of SheetConfiguration objects")
        else:
            for i, sheet_conf in enumerate(self.sheets):
                if not isinstance(sheet_conf, SheetConfiguration):
                    errors.append(f"Item at index {i} in MultipleSheetConfiguration.sheets is not a SheetConfiguration object")
                else:
                    errors.extend(sheet_conf.validate())
        if self.load_params and not isinstance(self.load_params, dict):
            errors.append("MultipleSheetConfiguration.load_params must be a dictionary")
        return errors
