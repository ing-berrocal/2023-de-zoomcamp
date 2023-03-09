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


## Week 1

## Week 2

## Week 3 - Data Warehouse and BigQuery

### Data Warehouse

OLAP
Used for reporting and data analysis, raw data, metadata and summary.

Data Marts ?

### BigQuery
Data warehouse solution, serveless

scalability and high-availability

##### Tablas Agrupdas

Las tablas agrupadas en BigQuery son tablas que tienen un orden de clasificación de columnas definido por el usuario mediante columnas agrupadas. Las tablas agrupadas pueden mejorar el rendimiento de las consultas y reducir los costos.

https://cloud.google.com/bigquery/docs/clustered-tables?hl=es-419

##### Partitions 
Dividir la tabla en base en un atributo, esto permite realizar busquedas mas rapidas y ahorra en constos.

##### Clustering

Agrupa la tabla por vcolumnas particionadas

#### Best Practice

- Evitar 'SELECT * '
- Consulta los precios antes de ejecutarlos
- Particionar o agrupar (Clustering) las tables
- insertra doatos con precaucion
- Materializar consultas en estages 



# WEEK 5

Bacth Processing

Bacth Jobs

Weekly
Daily
Hourly
3 Time pero Hour
Every 5 minutes


Technologies

Pyhton Scripts
SQL
Spark
Flink

Advantage

Easy to manage
Retry
Scale

Disvantage

Delay


### Apache Spark

Data processing engine

Batch Jobs or Streaming

 Java & Scala
 Python
 R

When to use Spark

Data Lake (S3/GCP, CSV, Parquet)


