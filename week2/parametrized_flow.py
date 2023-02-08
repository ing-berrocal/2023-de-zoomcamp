from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from prefect_gcp import GcpCredentials

def etl_web_to_gcs() -> None:
    """The main ETL function"""
    
    color = "yellow"
    year = 2021
    month = 1
    datase_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/ny-tlc-data/releases/download/{color}/{datase_file}.csv.gz"
    

if __name__ == '__main__':
    etl_web_to_gcs()