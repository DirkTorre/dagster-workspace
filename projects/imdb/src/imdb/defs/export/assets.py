from datetime import datetime

import dagster as dg
import requests
import subprocess
from pathlib import Path
from imdb.defs.ingestion.file_configs import FileConfig, FILE_CONFIGS

@dg.asset(
    name="movie_backup",
    description="Export movie data backup to cloud service.",
    group_name="export",
    # automation_condition=dg.AutomationCondition.on_cron("@daily"),
    required_resource_keys={
        "file_registry",
        "postgres",
    },
)
def movie_backup(context: dg.AssetExecutionContext):

    pr = context.resources.postgres

    pre_load_message = "Starting backup of imdb.watch_status and imdb.watch_date_scores..."
    context.log.info(pre_load_message)

    query = "SELECT * FROM imdb.watch_status;"""
    watch_status = pr.get_query_results(
        context,
        query,
    )
    query = "SELECT * FROM imdb.watch_date_scores;"""
    watch_date_scores = pr.get_query_results(
        context,
        query,
    )

    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S")

    Path("./backups").mkdir(parents=True, exist_ok=True)

    watch_status_path = Path("backups",f"{timestamp_str}_watch_status.parquet")
    watch_date_scores_path = Path("backups",f"{timestamp_str}_watch_date_scores.parquet")

    watch_status.write_parquet(watch_status_path)
    watch_date_scores.write_parquet(watch_date_scores_path)
    context.log.info(f"Backup completed: {watch_status_path} and {watch_date_scores_path}")

    # TODO: upload to cloud storage and return the cloud URLs instead of local paths
    return dg.MaterializeResult(
        metadata={
            "watch_status_backup_path": str(watch_status_path),
            "watch_date_scores_backup_path": str(watch_date_scores_path),
        }
    )