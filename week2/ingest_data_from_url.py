

import os
import pandas as pd
#Modulo para conexion con BD, se debe tener libreria python para conexion postgres
from sqlalchemy import create_engine
from time import time
from fastparquet import ParquetFile as pf

import argparse

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

csv_name = 'output.csv' 
parquet_name = 'output.parquet'  

@task#(log_prints=True,retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_file(url):

    os.system(f'wget {url} -O {parquet_name} --no-check-certificate')

    pfData = pf(parquet_name)
    data = pfData.to_pandas()

    data.columns = data.columns = [ x.lower() for x in data.columns]
    data.to_csv(csv_name,index=False)
    del data

    return csv_name

@task#(log_prints=True)
def conn_db(params):
    user = 'taxi_ny'
    host = '192.168.1.23'
    port = '5432'
    db = 'taxi_ny'
    password = 'taxi_ny'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    return engine


@task#(log_prints=True)
def main(params,engine):

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

@task#(log_prints=True)
def transform_data(df):
    print(f"pre:missiong passenger count: {df:['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count' != 0]]
    print(f"post:missiong passenger count: {df:['passenger_count'].isin([0]).sum()}")
    return df

@flow#(name="Ingest Flow")
def main_flow(params):
    
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
    download_file(url)

    engine = conn_db(params)

    main(params,engine)


if __name__ == '__main__' : 

    parser = argparse.ArgumentParser(description='ingest CSV data to Postgres')

    # user
    # password
    # host
    # port 
    # database name
    # table name
    # url of the csv

    #parser.add_argument('user',help='usename for postgres',default='taxi_ny',required=False)
    #parser.add_argument('password',help='password for postgres',default='taxi_ny',required=False)
    #parser.add_argument('host',help='host for postgres',default='192.168.1.23',required=False)
    #parser.add_argument('port',help='port for postgres',default='5432',required=False)
    #parser.add_argument('db',help='database name for postgres',default='taxi_ny',required=False)
    #parser.add_argument('table_name',help='Table name for postgres database where we will write the data',default='yellow_taxi_trip',required=False)
    #parser.add_argument('url',help='url of the csv file',default='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet',required=False)


    #args = parser.parse_args()

    flow.('flow') as flow :
        
    
    main_flow([])
#taxi_ny taxi_ny 192.168.1.23 5432 taxi_ny yellow_taxi_trip 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'