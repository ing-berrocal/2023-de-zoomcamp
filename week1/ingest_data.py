 #!/usr/bin/env python

import os
import pandas as pd
#Modulo para conexion con BD, se debe tener libreria python para conexion postgres
from sqlalchemy import create_engine
from time import time

import argparse


def main(params):

    user = params.user
    host = params.host
    port = params.port
    db = params.db
    table_name  = params.table_name
    url = params.url
    password = params.password  

    csv_name = 'output.csv'  

    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

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

        df.to_sql(name='yellow_taxi_data',con=engine,if_exists='append')

        t_end = time()
        
        print('Insert {}, time {}'.format(i,(t_end - t_start)))
    

if __name__ == '__main__' : 
    parser = argparse.ArgumentParser(description='ingest CSV data to Postgres')

    # user
    # password
    # host
    # port 
    # database name
    # table name
    # url of the csv

    parser.add_argument('user',help='usename for postgres')
    parser.add_argument('password',help='password for postgres')
    parser.add_argument('host',help='host for postgres')
    parser.add_argument('port',help='port for postgres')
    parser.add_argument('db',help='database name for postgres')
    parser.add_argument('table_name',help='Table name for postgres database where we will write the data')
    parser.add_argument('url',help='url of the csv file')


    args = parser.parse_args()

    main(args)



    


