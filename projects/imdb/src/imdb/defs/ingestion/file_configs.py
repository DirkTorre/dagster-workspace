from dataclasses import dataclass
from typing import List

@dataclass
class FileConfig:
    """Configuration for each downloadable file"""
    asset_name: str
    description: str
    url: str
    file_path: str
    group_name: str = "data_ingestion"

# Define all your file configurations
FILE_CONFIGS = [
    FileConfig(
        asset_name="title_akas",
        description="IMDB title akas dataset containing alternative titles",
        url="https://datasets.imdbws.com/title.akas.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.akas.tsv.gz"
    ),
    FileConfig(
        asset_name="title_basics",
        description="IMDB title basics dataset containing movie metadata",
        url="https://datasets.imdbws.com/title.basics.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.basics.tsv.gz"
    ),
    FileConfig(
        asset_name="title_crew", 
        description="IMDB title crew dataset containing cast and crew information",
        url="https://datasets.imdbws.com/title.crew.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.crew.tsv.gz"
    ),
    FileConfig(
        asset_name="title_episode", 
        description="IMDB title episodes dataset containing episode information",
        url="https://datasets.imdbws.com/title.episode.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.episode.tsv.gz"
    ),
    FileConfig(
        asset_name="title_principals", 
        description="IMDB title principals dataset containing principal cast and crew information",
        url="https://datasets.imdbws.com/title.principals.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.principals.tsv.gz"
    ),
    FileConfig(
        asset_name="title_ratings", 
        description="IMDB title ratings dataset with user scores",
        url="https://datasets.imdbws.com/title.ratings.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/title.ratings.tsv.gz"
    ),
    FileConfig(
        asset_name="name_basics", 
        description="IMDB name basics dataset containing actor and director information",
        url="https://datasets.imdbws.com/name.basics.tsv.gz",
        file_path="data/imdb/inputs/imdb_files/name.basics.tsv.gz"
    ),

    # FileConfig(
    #     asset_name="dates_and_scores",
    #     description="Custom dates and scores dataset",
    #     url="https://example.com/date_scores.csv",
    #     file_path="data/inputs/handmade_files/date_scores.csv"
    # ),
    # FileConfig(
    #     asset_name="status",
    #     description="Status tracking dataset",
    #     url="https://example.com/status.csv",
    #     file_path="data/inputs/handmade_files/status.csv"
    # ),
]