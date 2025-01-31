from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 31),  # Adjust accordingly
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the API URL for the daily transactions summary
API_URL_DAILY_SUMMARY = "http://10.70.1.13:9090/api/v1/daily-transactions-summary/"

# Function to fetch daily transactions summary
def fetch_daily_summary():
    try:
        formatted_date = datetime.now().strftime('%Y-%m-%d')
        response = requests.get(f"{API_URL_DAILY_SUMMARY}{formatted_date}", timeout=10)
        response.raise_for_status()
        summary = response.json()
        
        if summary:
            print(f"Daily summary retrieved for {formatted_date}: {summary}")
            return summary
        else:
            print("No daily summary available.")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching daily transactions summary: {e}")
        raise

# Define the DAG
with DAG(
    'daily_transactions_summary',
    default_args=default_args,
   schedule_interval='0 8,14,20 * * *',  # Runs twice a day at 09:00 and 21:00
    catchup=False,
    tags=['daily', 'transactions', 'summary']
) as dag:

    get_daily_summary = PythonOperator(
        task_id='fetch_daily_summary',
        python_callable=fetch_daily_summary,
        provide_context=True
    )

    get_daily_summary
