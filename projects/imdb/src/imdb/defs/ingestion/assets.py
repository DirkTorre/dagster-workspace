import dagster as dg
import requests
from pathlib import Path
from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS
from .resources import FileRegistry

def create_download_asset(config: FileConfig):
    """Factory function to create download assets from config"""
    
    @dg.asset(
        name=f"{config.asset_name}_raw",
        description=config.description,
        group_name=config.group_name,
        automation_condition=dg.AutomationCondition.on_cron("@daily"),
    )
    def download_asset(context: dg.AssetExecutionContext):
        """Download file from URL and save to disk"""
        
        # Create directory if it doesn't exist
        file_path = Path(config.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download the file
        context.log.info(f"Downloading from {config.url}")
        response = requests.get(config.url)
        response.raise_for_status()
        
        # Save to disk
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        context.log.info(f"Saved {config.asset_name} to {file_path}")
        
        # Return metadata about the download
        return dg.MaterializeResult(
            metadata={
                "file_size_bytes": len(response.content),
                "download_url": config.url,
                "local_path": str(file_path),
            }
        )
    
    return download_asset

# Create all download assets
download_assets = [create_download_asset(config) for config in FILE_CONFIGS]


