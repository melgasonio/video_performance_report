"""
This DAG queries video performance data of these two videos:
https://www.youtube.com/watch?v=BpEdIm-IBCk
https://www.youtube.com/watch?v=a-jhSWSjoMA
from the Youtube API and print's each video statistics.
"""

# This DAG uses the Taskflow API.
from airflow.sdk import Asset, dag, task
from pendulum import datetime, duration
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, LoadJobConfig, SchemaField
from google.cloud import bigquery
from tabulate import tabulate
import pandas as pd
import logging
import os
import sys

# Ensure /opt/airflow is on sys.path
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow')))

# Modularized helper functions
from include.custom_functions.video_functions import get_videos_stats, transform_df

# Use the Airflow task logger to log information to the task logs
t_log = logging.getLogger("airflow.task")

_BQ_TABLE_ID = os.getenv("BQ_TABLE_ID")

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
        outlets=[Asset("new_BQ_records")]
    ) # Define that this task produces updates to an Airflow dataset
    def load_video_data(
        transformed_df: pd.DataFrame,
        table_id: str = _BQ_TABLE_ID,
    ):
        t_log.info("Loading data into BigQuery")
            
        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            use_legacy_sql=False
        )
        
        client = hook.get_client()
        
        job_config = LoadJobConfig(
            schema=[
                SchemaField("id", "STRING"),
                SchemaField("title", "STRING"),
                SchemaField("channel", "STRING"),
                SchemaField("views", "INTEGER"),
                SchemaField("comments", "INTEGER"),
                SchemaField("likes", "INTEGER"),
            ],
            write_disposition="WRITE_TRUNCATE"
        )
        job = client.load_table_from_dataframe(
            transformed_df,
            table_id,
            job_config=job_config
        )
        job.result()
        
        t_log.info("Data loaded to BigQuery")
        
    # Prints loaded data to DuckDB
    @task
    def print_loaded_data(
        table_id: str = _BQ_TABLE_ID
    ):
        hook = BigQueryHook(
            gcp_conn_id="google_cloud_default",
            use_legacy_sql=False
        )
        
        client = hook.get_client() 

        query = f"SELECT * FROM `{table_id}` LIMIT 30" # Show data for the last 30 days
        loaded_data_df = client.query(query).to_dataframe()

        t_log.info(tabulate(loaded_data_df, headers="keys", tablefmt="pretty"))
        
    extracted = extract_video_data()
    transformed = transform_video_data(extracted)
    loaded = load_video_data(transformed)
     
    loaded >> print_loaded_data()
# Instantiate the DAG
video_data_etl()
    
    
    
