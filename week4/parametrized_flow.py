from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from prefect_gcp import GcpCredentials

#wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

@task(log_prints=True)
def p(color: str, year: str, month: str):
    """Download trip data from GCP"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("blockdezoom")

    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    #gcp_cloud_storage_bucket_block.download_folder_to_path(gcs_path,gcs_path)

    gcp_cloud_storage_bucket_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    
    return (f"./data/{gcs_path}")

@task(log_prints=True)
def process_yellow(csv_name,engine):

    table_name  = 'yellow_taxi_trip'
    
    df_iterator = pd.read_csv(csv_name,iterator=True,chunksize=100000)

    df = next(df_iterator)

    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

    df.to_sql(name=table_name,con=engine,if_exists='append')

    i = 0
    while True:
        i += 1
        t_start = time()

        df = next(df_iterator)
        df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
        df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name,con=engine,if_exists='append')

        t_end = time()
        
        print('Insert {}, time {}'.format(i,(t_end - t_start)))

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
def etl_gcs_to_bq(months : list(1,2)):
    """Main ETL flow to load data into BigQuery"""

    color="yellow"
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
    for year in [2019,2020]:
     
        for month in range(1,13):
            
            path = extract_from_gcs(color, year, month)

        file_name = f'{color}_tripdata_{year}-{month:02}.csv.gz'      
        url_tmp = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}'    
        #path_file = download_file(url_tmp,color,file_name)

    color="yellow"
    year=2021
    month=1

    for month in months:
        
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df)

if __name__=='__main__':
    etl_gcs_to_bq()        