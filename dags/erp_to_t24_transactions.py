from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 31),  # Adjust accordingly
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the API URL
API_URL = "http://10.70.1.13:9090/api/v1/transactions-from-erp-to-T24"

# Function to fetch transactions
def fetch_transactions():
    try:
        response = requests.get(API_URL, timeout=10)  # 10s timeout
        response.raise_for_status()
        transactions = response.json()
        
        if transactions:
            print(f"Retrieved {len(transactions)} transactions from ERP.")
            # Save to XCom or further process
            return transactions
        else:
            print("No transactions received.")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching transactions: {e}")
        raise

# Define the DAG
with DAG(
    'erp_to_t24_transactions',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=['erp', 't24', 'integration']
) as dag:

    get_transactions = PythonOperator(
        task_id='fetch_transactions',
        python_callable=fetch_transactions,
        provide_context=True
    )

    get_transactions
