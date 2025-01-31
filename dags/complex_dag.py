from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import sqlite3
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'complex_api_database_dag',
    default_args=default_args,
    description='A complex DAG that pulls data from an API, stores in a simulated DB, processes and posts to another API',
    schedule_interval=timedelta(days=1),
)

# Task 1: Fetch data from an API
def fetch_api_data(**kwargs):
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    logging.info(f"API Response Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        logging.info(f"Fetched API Data: {json.dumps(data[:5], indent=2)}")
        kwargs['ti'].xcom_push(key='api_data', value=data)
    else:
        logging.error("Failed to fetch API data")

task1 = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_api_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Store data in SQLite
def store_data_in_db(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_api_data', key='api_data')
    logging.info(f"Data received for storage: {json.dumps(data[:5], indent=2)}")
    conn = sqlite3.connect("airflow_simulated.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS api_data (id INTEGER, title TEXT, body TEXT)")
    for item in data[:10]:  # Limit to 10 rows for simulation
        cur.execute("INSERT INTO api_data (id, title, body) VALUES (?, ?, ?)", (item['id'], item['title'], item['body']))
    conn.commit()
    conn.close()
    logging.info("Data stored successfully in SQLite")

task2 = PythonOperator(
    task_id='store_data_in_db',
    python_callable=store_data_in_db,
    provide_context=True,
    dag=dag,
)

# Task 3: Process data
def process_data(**kwargs):
    conn = sqlite3.connect("airflow_simulated.db")
    cur = conn.cursor()
    cur.execute("SELECT * FROM api_data")
    processed_data = [{"id": row[0], "title": row[1].upper(), "body": row[2].upper()} for row in cur.fetchall()]
    conn.close()
    logging.info(f"Processed Data: {json.dumps(processed_data[:5], indent=2)}")
    kwargs['ti'].xcom_push(key='processed_data', value=processed_data)

task3 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Additional parallel tasks
def analyze_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Analyzing data: {json.dumps(data[:5], indent=2)}")

def clean_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Cleaning data: {json.dumps(data[:5], indent=2)}")

def validate_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Validating data: {json.dumps(data[:5], indent=2)}")

def summarize_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Summarizing data: {json.dumps(data[:5], indent=2)}")

task4 = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

task7 = PythonOperator(
    task_id='summarize_data',
    python_callable=summarize_data,
    provide_context=True,
    dag=dag,
)

# Task 8: Post processed data
def post_processed_data(**kwargs):
    url = "https://jsonplaceholder.typicode.com/posts"
    processed_data = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Posting Processed Data: {json.dumps(processed_data[:5], indent=2)}")
    for item in processed_data:
        response = requests.post(url, json=item)
        logging.info(f"Posted Data ID {item['id']} - Status: {response.status_code}")

task8 = PythonOperator(
    task_id='post_processed_data',
    python_callable=post_processed_data,
    provide_context=True,
    dag=dag,
)

# Define dependencies
task1 >> task2 >> task3 >> [task4, task5, task6, task7] >> task8
