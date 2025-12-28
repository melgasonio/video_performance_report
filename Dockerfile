FROM apache/airflow:3.1.5-python3.11
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt