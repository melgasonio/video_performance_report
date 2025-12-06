"""
This DAG queries video performance data of these two videos:
https://www.youtube.com/watch?v=BpEdIm-IBCk
https://www.youtube.com/watch?v=a-jhSWSjoMA
from the Youtube API and print's each video statistics.
"""

# This DAG uses the Taskflow API.
from airflow.sdk import Asset, chain, Param, dag, task
from pendulum import datetime, duration
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os

# Modularized helper functions
from include.custom_functions.video_functions import get_videos_stats

t_log = logging.getLogger("airflow.task")

@dag(
    start_date=datetime(2025, 12, 8),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md = __doc__,
    default_args={
        "owner": "Melga Sonio",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["data project", "ETL"],
    is_paused_upon_creation=True,
)
def video_data_etl():
    
    # Setup task
    @task
    def create_perf_table_in_duckdb(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        t_log.info("Creating videos performance table in DuckDB")
        
        cursor = duckdb.connect(duckdb_instance_name)
        
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                name STRING,
                view INT,
                comments INT,
                likes INT,
            )"""
        )
        cursor.close()
        
        t_log.info(f"Table {table_name} created in DuckDB")
        
    # Extract task
    @task
    def extract_video_data(num_videos: int = _NUM_VIDEOS_TOTAL) -> pd. DataFrame:
        videos_df = get_videos_stats(num_videos)
        
        return videos_df
    
    # Transform task
    @task
    def transform_video_data(
        videos_df: pd.DataFrame, 
        stats_list: list = _STATS_LIST
    ) -> pd.DataFrame:
        tranformed_df = transform_by_stats(stats_list)
        
        t_log.info("Transforming dataset based on specific stats needed")
        
        return tranformed_df
    
    # Load task
    @task
    def load_video_data(
        transformed_df: pd.DataFrame,
        duckdb_instance_name: str = _DUCK_DB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME
    ):
        t_log.info("Loading video data into DuckDB")
        cursor = duckdb.connect(duckdb_instance_name)
        cursor.sql(
            f"INSERT OR IGNORE INTO {table_name} BY ID SELECT * FROM transformed_df"
        )
        
        t_log.info("Videos data loaded into DuckDB")
        
    
    
    
    
