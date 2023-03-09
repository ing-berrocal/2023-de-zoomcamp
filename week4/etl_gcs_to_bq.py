from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from prefect_gcp import GcpCredentials

#wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

@task(log_prints=True)
def extract_from_gcs(color: str, year: str, month: str):
    """Download trip data from GCP"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("blockdezoom")

    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    #gcp_cloud_storage_bucket_block.download_folder_to_path(gcs_path,gcs_path)

    gcp_cloud_storage_bucket_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    
    return Path(f"./data/{gcs_path}")

@task(retries=3)
def fetch(url : str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    df = pd.read_csv(url)

    return df

@task
def write_local(df: pd.DataFrame, color: str, dataset_file) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path   

@task
def transform(pt: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(pt)

    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None :
    """Write Dataframe to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("blockzezoomcred")

    df.to_gbq(
        destination_table='test_yellow.rides',
        project_id='dtc-de-zoom',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=5000_000,
        if_exists='replace'
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into BigQuery"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color, year, month)

    df = transform(path)

    write_bq(df)

if __name__=='__main__':
    etl_gcs_to_bq()        