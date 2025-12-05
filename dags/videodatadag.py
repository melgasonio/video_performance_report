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

