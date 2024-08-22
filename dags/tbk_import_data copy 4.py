import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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

# Function to insert data from Excel into MySQL using SQLAlchemy ORM
def insert_data_from_excel(url):
    try:
        # Read Excel file into pandas DataFrame
        df = pd.read_excel(url)
        
        # Convert DataFrame to list of dictionaries
        data = df.to_dict(orient='records')

        # Create a session to interact with the database
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insert data into the database
        session.bulk_insert_mappings(TeaGrowers, data)
        session.commit()
        print("Data inserted successfully into the TeaGrowers_343 table.")

    except Exception as e:
        print(f"Error inserting data: {e}")

    finally:
        if session:
            session.close()
            print("Session closed.")

# Call the function to insert data from the Excel file
insert_data_from_excel(file_url)
