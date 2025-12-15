"""
This DAG queries video performance data of these two videos:
https://www.youtube.com/watch?v=BpEdIm-IBCk
https://www.youtube.com/watch?v=a-jhSWSjoMA
from the Youtube API and print's each video statistics.
"""

# This DAG uses the Taskflow API.
from airflow.sdk import Asset, chain, Param, dag, task
from pendulum import datetime, duration, timezone
from google.cloud import bigquery
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

_DUCKDB_INSTANCE_NAME = os.getenv("DUCK_DB_INSTANCE_NAME", "performance.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "performance_data")

# _BQ_PROJECT = os.getenv("BQ_PROJECT")
# _BQ_DATASET = os.getenv("BQ_DATASET")
# _BQ_TABLE = os.getenv("BQ_TABLE")
# _BQ_TABLE_URI = f"{_BQ_PROJECT}.{_BQ_DATASET}.{_BQ_TABLE}"

@dag(
    start_date=datetime(2025, 12, 8),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md = __doc__,
    default_args={
        "owner": "Melga Sonio",
        "retries": 3,
        "retry_delay": duration(seconds=20),
    },
    tags=["data project", "ETL"],
    is_paused_upon_creation=False,
)
def video_data_etl():
        
    # Extract task
    @task
    def extract_video_data() -> pd.DataFrame:
        videos_df = get_videos_stats()
        
        t_log.info("\n" + tabulate(videos_df, headers="keys", tablefmt="pretty", showindex=False))
        
        return videos_df
    
    # Transform task
    @task
    def transform_video_data(data_df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = transform_df(data_df)
        
        t_log.info("Transforming dataset based on specific stats needed")
        t_log.info("\n" + tabulate(transformed_df, headers="keys", tablefmt="pretty", showindex=False))
        
        return transformed_df
    
    # Load task
    @task(
        outlets=[Asset(_DUCKDB_INSTANCE_NAME)]
    ) # Define that this task produces updates to an Airflow dataset
    def load_video_data(
        transformed_df: pd.DataFrame,
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        duckdb_table_name: str = _DUCKDB_TABLE_NAME,
        # target_db: str = _TARGET_DB,
        # bq_project: str = _BQ_PROJECT,
        # bq_dataset: str = _BQ_DATASET,
        # bq_table: str = _BQ_TABLE,
    ):
        t_log.info("Loading video data into DuckDB")
        cursor = duckdb.connect(duckdb_instance_name)
        
        # Conditional creation: create table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {duckdb_table_name} (
                id VARCHAR PRIMARY KEY,
                title VARCHAR,
                channel VARCHAR,
                views INT,
                comments INT,
                likes INT
            );
            """
        )
        
        # Insert or replace data
        cursor.register("df", transformed_df)
        cursor.sql(
            f"INSERT INTO {duckdb_table_name} SELECT * FROM df;"
        )
        cursor.close()
        t_log.info("Videos data loaded into DuckDB")


        # if target_db == "duckdb":
        #     t_log.info("Loading video data into DuckDB")
        #     cursor = duckdb.connect(duckdb_instance_name)
            
        #     # Conditional creation: create table if it doesn't exist
        #     cursor.execute(
        #         f"""
        #         CREATE TABLE IF NOT EXISTS {duckdb_table_name} (
        #             id VARCHAR PRIMARY KEY,
        #             title VARCHAR,
        #             channel VARCHAR,
        #             views INT,
        #             comments INT,
        #             likes INT
        #         );
        #         """
        #     )
            
        #     # Insert or replace data
        #     cursor.register("df", transformed_df)
        #     cursor.sql(
        #         f"INSERT OR REPLACE INTO {duckdb_table_name} SELECT * FROM df;"
        #     )
        #     cursor.close()
        #     t_log.info("Videos data loaded into DuckDB")
        # else:
        #     t_log.info("Loading data into BigQuery")
            
        #     client = bigquery.Client(project=bq_project)
        #     table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
        #     job = client.load_table_from_dataframe(
        #         transformed_df,
        #         table_id,
        #         job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        #     )
        #     job.result()
        #     t_log.info("Data loadd to BigQuery")
        
    # Prints loaded data to DuckDB
    @task
    def print_loaded_data(
        # target_db: str = _TARGET_DB,
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        duckdb_table_name: str = _DUCKDB_TABLE_NAME,
        # bq_project: str = _BQ_PROJECT,
        # bq_dataset: str = _BQ_DATASET,
        # bq_table: str = _BQ_TABLE
    ):
        cursor = duckdb.connect(duckdb_instance_name)
        loaded_data_df = cursor.sql(f"SELECT * FROM {duckdb_table_name};").df()
        cursor.close()
        # if target_db == "duckdb":
        #     cursor = duckdb.connect(duckdb_instance_name)
        #     loaded_data_df = cursor.sql(f"SELECT * FROM {duckdb_table_name};").df()
        #     cursor.close()
        # else:
        #     client = bigquery.Client(project=bq_project)
        #     table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
        #     query = f"SELECT * FROM `{table_id}` LIMIT 30" # Show data for the last 30 days
        #     loaded_data_df = client.query(query).to_dataframe()

        t_log.info(tabulate(loaded_data_df, headers="keys", tablefmt="pretty"))
        
    extracted = extract_video_data()
    transformed = transform_video_data(extracted)
    loaded = load_video_data(transformed)
     
    loaded >> print_loaded_data()
# Instantiate the DAG
video_data_etl()
    
    
    
