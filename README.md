# 2023-de-zoomcamp


Data:
Pagina

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet

Diccionario
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

python ingest_data_from_url.py taxi_ny taxi_ny 192.168.1.22 5432 taxi_ny yellow_taxi_trip 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'


Instalación de Terrafor


Creación de cuenta en GCP

 1. Creamos el proyecto en GCP
 2. Creamos usuario en GCP
 3. Descargamos el archivo de conf

python ingest_data_from_url.py taxi_ny taxi_ny 192.168.1.24 5432 taxi_ny green_taxi_trip 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

 docker run -it --rm ingest:v0.0 taxi_ny taxi_ny 192.168.1.24 5432 taxi_ny green_taxi_trip 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet'
