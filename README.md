# T3-Pipeline
## Truck Transaction ETL 
### Overview
This project implements an ETL (Extract, Transform, Load) pipeline for processing truck transaction data. The pipeline extracts CSV files from an AWS S3 bucket, transforms and combines the data, and loads it into a Redshift database.

# Project Structure:
├── Extract.py          # Handles data extraction from S3
├── Transform.py        # Combines and cleans CSV files
├── Load.py            # Loads data into Redshift
├── .env               # Environment variables (not included)
├── downloaded_files/  # Temporary storage for downloaded CSV files
└── Cleaned_files/    # Storage for processed CSV files
Prerequisites

Python 3.x
AWS Account with S3 access
Amazon Redshift cluster
Required Python packages:

boto3
pandas
psycopg2
python-dotenv
redshift_connector



Environment Variables
Create a .env file with the following variables:
CopyAWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
DB_HOST=your_redshift_host
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_PORT=your_database_port
Data Pipeline Process
1. Extract (Extract.py)

Connects to AWS S3 using provided credentials
Identifies relevant data files based on current date and time
Downloads truck transaction CSV files to local storage
Files are stored in a structured format: trucks/YYYY-MM/DD/HH/

2. Transform (Transform.py)

Combines multiple truck CSV files into a single file
Performs data cleaning operations:

Removes invalid total amounts
Ensures totals are between 0.0 and 100.0
Handles missing values


Saves cleaned data to Cleaned_files/combined_truck_data.csv

3. Load (Load.py)

Establishes connection to Redshift database
Maps payment types to payment method IDs
Uploads transformed data to the FACT_Transaction table
Handles data type conversions and validations

Usage
Run the entire pipeline:
bashCopypython Load.py
Or run individual components:
bashCopypython Extract.py  # Only download files
python Transform.py  # Only process files
Database Schema
The pipeline loads data into the following schema:
sqlCopy-- keogh_jokhan_schema
FACT_Transaction (
    truck_id INT,
    payment_method_id INT,
    total FLOAT,
    at TIMESTAMP
)

DIM_Payment_Method (
    payment_method_id INT,
    payment_method VARCHAR
)
Error Handling

The pipeline includes checks for:

Missing or invalid S3 data
Data validation during transformation
Database connection issues


Errors are logged with appropriate messages

Notes

The pipeline is designed to process data from the last 3 hours
Files are automatically cleaned up after processing
Transactions outside the valid range (0.0-100.0) are filtered out
The pipeline is designed to run on a schedule (current date/time)