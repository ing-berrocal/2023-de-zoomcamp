import os
import pandas as pd
#Modulo para conexion con BD, se debe tener libreria python para conexion postgres
from sqlalchemy import create_engine
from time import time
from fastparquet import ParquetFile as pf
import wget

#import argparse

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from pathlib import Path

@task(log_prints=True,retries=3#, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
      )
def download_file(url,color,filename) -> Path: 
    #print(url)
    #pd.read_csv(url,compression='gzip')
    #os.system(f'wget {url} -O ./data/{color}/{filename} ') #--no-check-certificate
    wget.download(url,out = f'./data/{color}/{filename}')
    #pd.to_csv(f"./data/{color}/{filename}")
    return Path(f"./data/{color}/{filename}")

@task(log_prints=True)
def conn_db():
    user = 'postgres'
    host = '104.198.38.161'
    port = '5432'
    db = 'postgres'
    password = '6A8KVnp6QvZi3zd'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    return engine

@task(log_prints=True)
def insert_yellow(csv_name,engine):

    table_name  = 'yellow_taxi_trip'
    
    df_iterator = pd.read_csv(csv_name,iterator=True,chunksize=100000,compression='gzip')

    df = next(df_iterator)

    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name,con=engine,if_exists='append')

    df.to_sql(name=table_name,con=engine,if_exists='append')

    try: 
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
    except Exception as err:
       print('Error....')

@task(log_prints=True)
def insert_for(csv_name,engine):

    table_name  = 'yellow_taxi_trip'
    
    df_iterator = pd.read_csv(csv_name,chunksize=100000)

    for df in df_iterator:

      t_start = time()

      df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
      df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)

      df.head(0).to_sql(name=table_name,con=engine,if_exists='apppend')

      df.to_sql(name=table_name,con=engine,if_exists='append')

      t_end = time()

      print('Insert {}, time {}'.format(0,(t_end - t_start)))


@flow(name="Ingest Flow")
def fn_flow():

  color="yellow"
  # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
  for year in [2019,2020]:
     
    for month in range(1,13):
      file_name = f'{color}_tripdata_{year}-{month:02}.csv.gz'      
      url_tmp = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}'    
      #path_file = download_file(url_tmp,color,file_name)

      engine = conn_db()

      insert_yellow(f'./data/{color}/{color}_tripdata_{year}-{month:02}.csv.gz',engine)
      #path = extract_from_gcs(color, year, month)
      #df = transform(path)
      #write_bq(df)


    #url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
    #file_csv = download_file(url)

    print(f" : {path_file}")

    

  #insert_for(file_csv,engine)

if __name__ == '__main__' :
  fn_flow()