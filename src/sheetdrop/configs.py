
import os
import yaml

class AppConfig:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        self.database_schema = os.getenv("DATABASE_SCHEMA")
        self.storage_provider = os.getenv("STORAGE_PROVIDER")

        if not all([self.database_url, self.database_schema, self.storage_provider]):
            try:
                with open("config.yaml") as f:
                    yaml_config = yaml.safe_load(f)
                    self.database_url = self.database_url or yaml_config.get("database_url")
                    self.database_schema = self.database_schema or yaml_config.get("database_schema")
                    self.storage_provider = self.storage_provider or yaml_config.get("storage_provider")
            except FileNotFoundError:
                pass  # YAML is optional, environment variables can be used

        if not all([self.database_url, self.database_schema, self.storage_provider]):
            raise ValueError("Missing required configuration. Please set DATABASE_URL, DATABASE_SCHEMA, and STORAGE_PROVIDER environment variables or provide them in a config.yaml file.")

app_configs = AppConfig()
