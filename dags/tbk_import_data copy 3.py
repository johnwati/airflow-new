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

    # Rename DataFrame columns to match SQL table field names
    df.columns = [
        'Grower_Number', 'Grower_Name', 'NationalID', 'Grower_Group', 'Company_Registration_Certificate_No',
        'CompanyPIN', 'Gender', 'Date_of_Birth', 'TEl_Number', 'Email', 'Land_Registration_No', 'Total_Land_Area',
        'Tea_Cultivation_Area', 'Factory', 'Buying_Centre', 'Ward_Location', 'Sub_County', 'County', 'Regionid',
        'Tea_Varieties_Cultivated', 'Tea_Cultivars', 'Total_Tea_Bushes', 'Age_of_the_Tea_Bush', 'Productivity_per_Bush',
        'Type_of_Farming', 'Membership_in_Tea_Association', 'Total_Fertilizer_Per_Year_Acre', 'Average_Annual_Tea_Production',
        'Payment_Method', 'Date_Greenleaf_Agreement_Signed'
    ]
    return df

def create_table(cursor):
    """Create the tea_growers table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tea_growers_33 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Grower_Number VARCHAR(255),
        Grower_Name VARCHAR(255),
        NationalID VARCHAR(255),
        Grower_Group VARCHAR(255),
        Company_Registration_Certificate_No VARCHAR(255),
        CompanyPIN VARCHAR(255),
        Gender VARCHAR(255),
        Date_of_Birth VARCHAR(255),
        TEl_Number VARCHAR(255),
        Email VARCHAR(255),
        Land_Registration_No VARCHAR(255),
        Total_Land_Area VARCHAR(255),
        Tea_Cultivation_Area VARCHAR(255),
        Factory VARCHAR(255),
        Buying_Centre VARCHAR(255),
        Ward_Location VARCHAR(255),
        Sub_County VARCHAR(255),
        County VARCHAR(255),
        Regionid VARCHAR(255),
        Tea_Varieties_Cultivated VARCHAR(255),
        Tea_Cultivars VARCHAR(255),
        Total_Tea_Bushes VARCHAR(255),
        Age_of_the_Tea_Bush VARCHAR(255),
        Productivity_per_Bush VARCHAR(255),
        Type_of_Farming VARCHAR(255),
        Membership_in_Tea_Association VARCHAR(255),
        Total_Fertilizer_Per_Year_Acre VARCHAR(255),
        Average_Annual_Tea_Production VARCHAR(255),
        Payment_Method VARCHAR(255),
        Date_Greenleaf_Agreement_Signed VARCHAR(255)
    )
    """
    cursor.execute(create_table_query)

def insert_data(cursor, df):
    """Insert data from the DataFrame into the MySQL table."""
    insert_query = """
    INSERT INTO tea_growers_33 (
        Grower_Number, Grower_Name
    ) VALUES (%s, %s)
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
            host='41.220.116.115',  # e.g., 'localhost'
            database='tbk-core',
            user='tbk-core',
            password='tbk-core'
        )

        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create table
            # create_table(cursor)
            
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
