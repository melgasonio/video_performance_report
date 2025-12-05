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
    
