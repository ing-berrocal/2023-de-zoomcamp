import os
import pandas as pd
#Modulo para conexion con BD, se debe tener libreria python para conexion postgres
from sqlalchemy import create_engine
from time import time
from fastparquet import ParquetFile as pf

#import argparse

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True,retries=3#, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
      )
def download_file(url):
    
    csv_name = 'output.csv' 
    parquet_name = 'output.parquet'  

    os.system(f'wget {url} -O {parquet_name} --no-check-certificate')

    pfData = pf(parquet_name)
    data = pfData.to_pandas()

    data.columns = data.columns = [ x.lower() for x in data.columns]
    data.to_csv(csv_name,index=False)
    del data

    return csv_name

@task(log_prints=True)
def conn_db():
    user = 'taxi_ny'
    host = '192.168.1.23'
    port = '5432'
    db = 'taxi_ny'
    password = 'taxi_ny'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    return engine

@task(log_prints=True)
def insert(csv_name,engine):

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

@task(log_prints=True)
def insert_for(csv_name,engine):

    table_name  = 'yellow_taxi_trip'
    
    df_iterator = pd.read_csv(csv_name,chunksize=100000)

    for df in df_iterator:

      t_start = time()

      df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
      df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

      df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')

      df.to_sql(name=table_name,con=engine,if_exists='append')

      t_end = time()

      print('Insert {}, time {}'.format(0,(t_end - t_start)))


@flow(name="Ingest Flow")
def fn_flow():

  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
  url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
  file_csv = download_file(url)

  print(f" : {file_csv}")

  engine = conn_db()

  insert_for(file_csv,engine)

if __name__ == '__main__' :
  fn_flow()