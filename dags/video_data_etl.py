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
from include.custom_functions.video_functions import get_videos_stats, transform_df

# Use the Airflow task logger to log information to the task logs
t_log = logging.getLogger("airflow.task")

# Define variables used in a DAG as environment variables in .env for your Airflow instance to standardize your DAGs
_TARGET_DB = os.getenv("TARGET_DB", "duckdb")

_DUCKDB_INSTANCE_NAME = os.getenv("DUCK_DB_INSTANCE_NAME", "include/db/performance.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "performance_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"

@dag(
    start_date=datetime(2025, 12, 10),
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
                id VARCHAR PRIMARY KEY,
                title VARCHAR,
                channel VARCHAR,
                view INT,
                comments INT,
                likes INT
            );"""
        )
        cursor.close()
        
        t_log.info(f"Table {table_name} created in DuckDB")
        
    # Extract task
    @task
    def extract_video_data() -> pd.DataFrame:
        videos_df = get_videos_stats()
        
        return videos_df
    
    # Transform task
    @task
    def transform_video_data(videos_df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = transform_df(videos_df)
        
        t_log.info("Transforming dataset based on specific stats needed")
        
        return transformed_df
    
    # Load task
    @task(
        outlets=[Asset(_DUCKDB_TABLE_URI)]
    ) # Define that this task produces updates to an Airflow dataset
    def load_video_data(
        transformed_df: pd.DataFrame,
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME
    ):
        t_log.info("Loading video data into DuckDB")
        cursor = duckdb.connect(duckdb_instance_name)
        cursor.register("df", transformed_df)
        cursor.sql(
            f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df;"
        )
        
        t_log.info("Videos data loaded into DuckDB")
        
    # Prints loaded data to DuckDB
    @task
    def print_loaded_data(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        cursor = duckdb.connect(duckdb_instance_name)
        loaded_data_df = cursor.sql(f"SELECT * FROM {table_name};").df()

        t_log.info(tabulate(loaded_data_df, headers="keys", tablefmt="pretty"))
        
        
    create = create_perf_table_in_duckdb()
    extract = extract_video_data()
    transform = transform_video_data(extract)
    load = load_video_data(transform)
    printer = print_loaded_data()
    
    create >> extract >> transform >> load >> printer

# Instantiate the DAG
video_data_etl()
    
    
    
