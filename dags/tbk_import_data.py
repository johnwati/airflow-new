import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

# URL of the Excel file to download
file_url = "http://41.220.116.115/administrator/file/download/form_upload_tea_growers/20240530123449-2024-05-30form_upload_tea_growers123434.xls"

# SQLAlchemy database connection
db_url = 'mysql+mysqlconnector://tbk-core:tbk-core@41.220.116.115/tbk-core'
engine = create_engine(db_url, echo=True)  # Set echo=True for debugging

Base = declarative_base()

# Define the ORM model for TeaGrowers table
class TeaGrowers(Base):
    __tablename__ = 'TeaGrowers_343'

    id = Column(Integer, primary_key=True, autoincrement=True)
    GrowerNumber = Column(String(255))
    GrowerName = Column(String(255))
    NationalID = Column(String(255))
    GrowerGroup = Column(String(255))
    CompanyRegCertNo = Column(String(255))
    CompanyPIN = Column(String(255))
    Gender = Column(String(255))
    DateOfBirth = Column(String(255))  # Assuming date is stored as string from Excel
    TelNumber = Column(String(255))
    Email = Column(String(255))
    LandRegNo = Column(String(255))
    TotalLandArea = Column(String(255))
    TeaCultivationArea = Column(String(255))
    Factory = Column(String(255))
    BuyingCentre = Column(String(255))
    WardLocation = Column(String(255))
    SubCounty = Column(String(255))
    County = Column(String(255))
    Region = Column(String(255))
    TeaVarieties = Column(String(255))
    TeaCultivars = Column(String(255))
    TotalTeaBushes = Column(String(255))
    AgeOfTeaBushes = Column(String(255))
    ProductivityPerBush = Column(String(255))
    TypeOfFarming = Column(String(255))
    MembershipInTeaAssociation = Column(String(255))
    TotalFertilizerPerYear = Column(String(255))
    AvgAnnualTeaProduction = Column(String(255))
    PaymentMethod = Column(String(255))
    DateGreenleafAgreementSigned = Column(String(255))

Base.metadata.create_all(engine)

# Define the columns to read based on their positions in the Excel file
column_indices = [
    'GrowerNumber', 'GrowerName', 'NationalID', 'GrowerGroup', 'CompanyRegCertNo', 'CompanyPIN', 'Gender', 'DateOfBirth',
    'TelNumber', 'Email', 'LandRegNo', 'TotalLandArea', 'TeaCultivationArea', 'Factory', 'BuyingCentre', 'WardLocation',
    'SubCounty', 'County', 'Region', 'TeaVarieties', 'TeaCultivars', 'TotalTeaBushes', 'AgeOfTeaBushes', 'ProductivityPerBush',
    'TypeOfFarming', 'MembershipInTeaAssociation', 'TotalFertilizerPerYear', 'AvgAnnualTeaProduction', 'PaymentMethod',
    'DateGreenleafAgreementSigned'
]

# Function to insert data from Excel into MySQL using SQLAlchemy ORM
def insert_data(data_chunk):
    try:
        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insert data into the database
        session.bulk_insert_mappings(TeaGrowers, data_chunk)
        session.commit()
        print("Chunk inserted successfully into the TeaGrowers_343 table.")

    except Exception as e:
        print(f"Error inserting chunk: {e}")

    finally:
        if session:
            session.close()
            print("Session closed.")

# Function to read data from the Excel file and split it into chunks
def read_and_chunk_data(url, chunk_size=500):
    try:
        # Read Excel file into pandas DataFrame by column positions
        df = pd.read_excel(url, header=None, usecols=list(range(30)), names=column_indices)
        
        # Convert DataFrame to list of dictionaries
        data = df.where(pd.notna(df), None).to_dict(orient='records')
        
        # Split data into chunks
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
        
    except Exception as e:
        print(f"Error reading and chunking data: {e}")

# Main function to orchestrate the data insertion using multithreading
def main(url):
    try:
        data_chunks = read_and_chunk_data(url)

        # Use ThreadPoolExecutor to insert data in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(insert_data, data_chunks)

    except Exception as e:
        print(f"Error in main function: {e}")

# Call the main function to start the process
if __name__ == "__main__":
    main(file_url)
