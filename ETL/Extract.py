"""Python script to extract data from S3"""
import os
from datetime import datetime
from psycopg2.extensions import connection
from boto3 import client
from dotenv import load_dotenv

load_dotenv(".env")

def s3_connection() -> connection:
    """Connects to an S3"""
    s3 = client("s3", aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY"))
    return s3

def find_times_in_bucket(s3: connection, bucket: str, year_and_month: str, day: str,) -> list[int]:
    """Retrieves the list of times for the current day of when the data has been uploaded to S3"""
    times = []
    prefix = f"trucks/{year_and_month}/{day}"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
    try:
        for file in response["Contents"]:
            times.append(int(file['Key'].split("/")[3]))
        return list(set(times))
    except:
        return "No data in bucket"

def download_truck_data_files(s3: connection, bucket: str,
                              year_and_month: str, day: str, time: str) -> None:
    """Downloads relevant files from S3 to a data/ folder."""
    prefix = f"trucks/{year_and_month}/{day}/{time}"
    response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
    for file in response["Contents"]:

        file_name = file['Key']

        if file_name.endswith(".csv"):

            local_file_path = os.path.join("downloaded_files", file_name)

            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            s3.download_file(bucket,file_name,local_file_path)

def find_current_time_for_bucket_name(time: int, times: list[int]) -> str:
    """Finds the closest previous time bucket within the last 3 hours; otherwise returns None."""
    possible_time = times

    for hour in reversed(possible_time): 
        if 0 <= time - hour <= 3 or time == 0:
            return str(hour)
    return None

def get_basic_extract_information() -> list:
    """Gets the basic information for extracting data from a S3 bucket"""
    s3 = s3_connection()
    bucket = "sigma-resources-truck"
    now = datetime.now()

    year_and_month = now.strftime("%Y-%m")
    day = f"{now.day}"
    times_in_bucket = find_times_in_bucket(s3,bucket,year_and_month,day)
    try:
        time = find_current_time_for_bucket_name(int(now.hour),times_in_bucket)
        return [s3,bucket,year_and_month,day,time]
    except:
        return "No data in bucket"

if __name__ == "__main__":
    try:
        s3,bucket,year_and_month,day,time = get_basic_extract_information()
        download_truck_data_files(s3,bucket,year_and_month,day,time)
    except:
        print("No data in bucket")
