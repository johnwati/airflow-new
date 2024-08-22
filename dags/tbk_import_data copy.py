import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error

def download_excel_file(url, file_path):
    """Download the Excel file from the given URL."""
    response = requests.get(url)
    with open(file_path, 'wb') as file:
        file.write(response.content)

def read_excel_file(file_path):
    """Read the Excel file into a DataFrame."""
    df = pd.read_excel(file_path)
    return df

def create_table(cursor, columns):
    """Create the tea_growers table if it does not exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS tea_growers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join([f'column{i+1} VARCHAR(255)' for i in range(columns)])}
    )
    """
    cursor.execute(create_table_query)

def insert_data(cursor, df):
    """Insert data from the DataFrame into the MySQL table."""
    insert_query = f"""
    INSERT INTO tea_growers (
        {', '.join([f'column{i+1}' for i in range(len(df.columns))])}
    ) VALUES ({', '.join(['%s'] * len(df.columns))})
    """
    for i, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

def main():
    url = "http://41.220.116.115/administrator/file/download/form_upload_tea_growers/20240530123449-2024-05-30form_upload_tea_growers123434.xls"
    file_path = "tea_growers.xls"
    
    # Step 1: Download the Excel file
    download_excel_file(url, file_path)
    
    # Step 2: Read the Excel file
    df = read_excel_file(file_path)
    
    # Print the DataFrame to inspect its structure
    print(df.head())
    print(f"Number of columns: {len(df.columns)}")
    
    # Step 3: Connect to MySQL database and insert data
    try:
        connection = mysql.connector.connect(
            host='your_host',  # e.g., 'localhost'
            database='your_database',
            user='your_username',
            password='your_password'
        )

        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create table with the number of columns found in the DataFrame
            create_table(cursor, len(df.columns))
            
            # Insert data
            insert_data(cursor, df)
            
            # Commit the transaction
            connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()
