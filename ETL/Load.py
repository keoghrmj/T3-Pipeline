"""A script that imports data from S3, combines it into a single CSV and 
then uploads it to a Redshift database."""
import csv
import os
import redshift_connector
from dotenv import load_dotenv
from psycopg2.extensions import cursor, connection
from Extract import s3_connection, download_truck_data_files, get_basic_extract_information
from Transform import combine_transaction_data_files, clean_data

def get_connection() -> redshift_connector.Connection:
    """Creates a connection to the Redshift database"""
    load_dotenv(".env")
    return redshift_connector.connect(
        host=os.environ.get('DB_HOST').strip(),
        database=os.environ.get('DB_NAME').strip(),
        user=os.environ.get('DB_USER').strip(),
        password=os.environ.get('DB_PASSWORD').strip(),
        port=int(os.environ.get('DB_PORT').strip()))

def get_cursor(conn: redshift_connector.Connection) -> redshift_connector.Cursor:
    """Creates a cursor for the database"""
    return conn.cursor()

def get_payment_method_id(cursor: cursor, payment_type: str) -> int:
    """Retrieves payment_method_id from the database"""
    cursor.execute("SET search_path TO keogh_jokhan_schema;")
    cursor.execute("""SELECT payment_method_id
                   FROM DIM_Payment_Method 
                   WHERE payment_method = %s;""", (payment_type,))
    rows = cursor.fetchone()
    return rows[0]

def upload_transaction_data(csv_file_path: str) -> None:
    """Uploads transaction data to the database."""
    conn = get_connection()
    cursor = get_cursor(conn)
    cursor.execute("SET search_path TO keogh_jokhan_schema;")

    query = """
    INSERT INTO FACT_Transaction (truck_id, payment_method_id, total, at)
    VALUES (%s, %s, %s, %s);
    """
    with open(csv_file_path, encoding="UTF_8") as csv_file:

        reader = csv.DictReader(csv_file)

        for row in reader:
            truck_id = int(row['truck_id'])
            payment_method_id = get_payment_method_id(cursor,row['type'])
            total = float(row['total'])
            at = row['timestamp']

            values = (truck_id,payment_method_id,total,at)
            cursor.execute(query,values)
            conn.commit()
    conn.close()

def extract_data(s3: connection, bucket: str) -> None:
    """Extracts relevant files from S3 bucket."""
    try:
        s3,bucket,year_and_month,day,time = get_basic_extract_information()
        download_truck_data_files(s3,bucket,year_and_month,day,time)
        return [day, time]
    except:
        return "No data in bucket"

def transform_data(day: int, time: int) -> None:
    """Transforms relevant CSV files into one combined, cleaned CSV file"""
    directory_path = f"downloaded_files/trucks/2024-11/{day}/{time}"
    combined_csv_file = "combined_truck_data.csv"

    combined_csv = os.path.join("Cleaned_files",combined_csv_file)

    combine_transaction_data_files(directory_path,combined_csv)

    clean_data(combined_csv)


if __name__ == "__main__":
    s3 = s3_connection()
    bucket = "sigma-resources-truck"
    day,time = extract_data(s3, bucket)

    transform_data( day, time)

    csv_file = "Cleaned_files/combined_truck_data.csv"
    upload_transaction_data(csv_file)

