import dagster as dg




# Typically, resources are reusable objects that supply external context, 
# such as database connections, API clients, or configuration settings. 
# Because of this, a single resource can be shared across many different Dagster objects.

import dagster as dg
from typing import Dict, List
from pathlib import Path
from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS


class FileRegistry(dg.ConfigurableResource):
    """Registry of all file configurations"""
    
    def get_config(self, asset_name: str) -> FileConfig:
        """Get configuration for a specific asset"""
        configs_dict = {c.asset_name: c for c in FILE_CONFIGS}
        if asset_name not in configs_dict:
            raise ValueError(f"Unknown asset: {asset_name}")
        return configs_dict[asset_name]
    
    def get_path(self, asset_name: str) -> Path:
        """Get file path for a specific asset"""
        return Path(self.get_config(asset_name).file_path)
    
    def get_url(self, asset_name: str) -> str:
        """Get URL for a specific asset"""
        return self.get_config(asset_name).url
    
    @property
    def all_configs(self) -> List[FileConfig]:
        """Get all file configurations"""
        return FILE_CONFIGS


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "file_registry": FileRegistry(),
    })
