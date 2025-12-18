from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('github_deploy_test', start_date=datetime(2025, 1, 1), schedule='@once') as dag:
    t1 = BashOperator(task_id='confirm_success', bash_command='echo "Deployed via GitHub Actions!"')