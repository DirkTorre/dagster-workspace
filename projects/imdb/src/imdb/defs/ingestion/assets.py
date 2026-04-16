# import dagster as dg
# import requests
# from pathlib import Path
# from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS
# from .resources import FileRegistry

# def create_download_asset(config: FileConfig):
#     """Factory function to create download assets from config"""
    
#     @dg.asset(
#         name=f"{config.asset_name}_raw",
#         description=config.description,
#         group_name=config.group_name,
#         automation_condition=dg.AutomationCondition.on_cron("@daily"),
#     )
#     def download_asset(context: dg.AssetExecutionContext):
#         """Download file from URL and save to disk"""
        
#         # Create directory if it doesn't exist
#         file_path = Path(config.file_path)
#         file_path.parent.mkdir(parents=True, exist_ok=True)
        
#         # Download the file
#         context.log.info(f"Downloading from {config.url}")
#         response = requests.get(config.url)
#         response.raise_for_status()
        
#         # Save to disk
#         with open(file_path, 'wb') as f:
#             f.write(response.content)
        
#         context.log.info(f"Saved {config.asset_name} to {file_path}")
        
#         # Return metadata about the download
#         return dg.MaterializeResult(
#             metadata={
#                 "file_size_bytes": len(response.content),
#                 "download_url": config.url,
#                 "local_path": str(file_path),
#             }
#         )
    
#     return download_asset

# # Create all download assets
# download_assets = [create_download_asset(config) for config in FILE_CONFIGS]


import dagster as dg
import requests
from pathlib import Path
from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS

@dg.asset(
    name="imdb_download",
    description="Download all IMDB files together to ensure consistency",
    group_name="ingestion",  # You might want to adjust this group name
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def imdb_download(context: dg.AssetExecutionContext):
    """Download all files from URLs and save to disk"""
    
    download_results = {}
    total_bytes = 0
    
    for config in FILE_CONFIGS:
        # Create directory if it doesn't exist
        file_path = Path(config.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download the file
        context.log.info(f"Downloading {config.asset_name} from {config.url}")
        response = requests.get(config.url)
        response.raise_for_status()
        
        # Save to disk
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        file_size = len(response.content)
        total_bytes += file_size
        
        context.log.info(f"Saved {config.asset_name} to {file_path} ({file_size:,} bytes)")
        
        # Store results for each file
        download_results[config.asset_name] = {
            "file_size_bytes": file_size,
            "download_url": config.url,
            "local_path": str(file_path),
        }
    
    # Return metadata about all downloads
    return dg.MaterializeResult(
        metadata={
            "total_files_downloaded": len(FILE_CONFIGS),
            "total_bytes_downloaded": total_bytes,
            "files": download_results,
        }
    )