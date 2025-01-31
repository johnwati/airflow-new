from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the API call function
def call_api():
    url = "http://10.70.1.13:9090/api/v1/daily-currency-rates-summary"  # Replace with your API URL
    response = requests.get(url)
    if response.status_code == 200:
        print("API call successful!")
    else:
        print(f"API call failed with status code {response.status_code}")

# Define the DAG
with DAG(
    'daily-currency-rates-summary',
    default_args=default_args,
    description='A DAG to call an API every 2 minutes',
    schedule_interval='0 8,14,20 * * *',
    catchup=False,
) as dag:

    # Define the task
    api_call_task = PythonOperator(
        task_id='call_api',
        python_callable=call_api,
    )

    # Set task dependencies (if any)
    api_call_task
