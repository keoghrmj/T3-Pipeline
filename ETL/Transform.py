"""Python script to combine CSV files into a cleaned, single file"""
import os
import pandas as pd
from Extract import get_basic_extract_information

def combine_transaction_data_files(directory_path: str, combined_csv: str) -> None:
    """Loads and combines relevant files from the data/ folder.
    Produces a single combined file in the data/ folder."""

    truck_dataframes = []

    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path,filename)

            truck_data = pd.read_csv(file_path)
            truck_id = filename.split("_")[1][1]

            truck_data['truck_id'] = truck_id
            truck_dataframes.append(truck_data)
            os.remove(file_path)

    combined_data_frames = pd.concat(truck_dataframes, ignore_index=True)
    combined_data_frames.to_csv(combined_csv,index=False)

def clean_total(csv_file: str,truck_data: pd) -> pd:
    """Cleans the total column in the csv file"""
    truck_data['total'] = pd.to_numeric(truck_data['total'], errors='coerce')
    truck_data.dropna(subset=['total'], inplace=True)
    truck_data = truck_data[(truck_data['total'] > 0.0) & (truck_data['total'] < 100.0)]

    truck_data.to_csv(csv_file,index=False)
    return truck_data

def clean_data(csv_file: str) -> None:
    """Cleans the data in a CSV file"""
    truck_data = pd.read_csv(csv_file)
    truck_data = clean_total(csv_file,truck_data)
    truck_data.to_csv(csv_file,index=False)

if __name__ == "__main__":
    s3,bucket,year_and_month,day,time = get_basic_extract_information()

    directory_path = f"downloaded_files/trucks/2024-11/{day}/{time}"

    combined_csv_file = "combined_truck_data.csv"
    combined_csv = os.path.join("Cleaned_files",combined_csv_file)

    combine_transaction_data_files(directory_path,combined_csv)
    clean_data(combined_csv)