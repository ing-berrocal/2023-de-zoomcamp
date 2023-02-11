from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from random import randint

#wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz

@flow()
def etl_local_to_gcs(year: int, months : list) ->None:
    """The main ETL function"""

    
    for month in months:
        dataset_file = f"fhv_tripdata_{year}-{month:02}"        
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        #df_clean = clean(df)
        path_file = write_local(df, dataset_file)        
        path_file = write_gcp(path_file, dataset_file)

    #write_gcp(path_file)

@task
def write_local(df: pd.DataFrame, dataset_file) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"./data/fhv/{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path    

def write_gcp(path : Path, fileName: str) -> None:
    """Write DataFrame out locally as parquet file"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("blockdezoom")

    print(path)


    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path,to_path= Path(f"{fileName}.parquet"))
    return

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    #df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(retries=3)
def fetch(url : str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    df = pd.read_csv(url)
    return df

if __name__ == '__main__':
    
    year = 2019
    months = [1,2,3,4,5,6,7,8,9,10,11,12]

    etl_local_to_gcs(year, months)    