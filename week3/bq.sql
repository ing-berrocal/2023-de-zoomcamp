-- Creacion de tabla externa
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoom.week_3.external_trip_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://etl-de-zoom-dbn/homework/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoom.week_3.external_trip_data`
OPTIONS (
  format = 'CSV',
  compression = 'GZIP',
  uris = ['gs://etl-de-zoom-dbn/homework/fhv_tripdata_2019-*.csv.gz']
);

--Numero de registros, 43,244,696
SELECT count(*) FROM `dtc-de-zoom.week_3.external_trip_data` LIMIT 10;

--
SELECT * FROM `dtc-de-zoom.week_3.external_trip_data` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `dtc-de-zoom.week_3.table_trip_data_non_partitoned` AS
SELECT * FROM `dtc-de-zoom.week_3.external_trip_data`;

--external
-- 3.03 GB, 33 s
SELECT DISTINCT(Affiliated_base_number)
FROM dtc-de-zoom.week_3.external_trip_data;

--table
-- 317.94 MB, 33 s
SELECT DISTINCT(Affiliated_base_number)
FROM dtc-de-zoom.week_3.table_trip_data_non_partitoned;

-- 717748
SELECT count(*)
FROM dtc-de-zoom.week_3.table_trip_data_non_partitoned
where PUlocationID is null and DOlocationID is null;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-zoom.week_3.table_trip_data_partitoned_clustered
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM dtc-de-zoom.week_3.table_trip_data_non_partitoned;

--23.05 MB
select distinct(affiliated_base_number) from dtc-de-zoom.week_3.table_trip_data_partitoned_clustered
where pickup_datetime between '2019-03-01 00:00:00' and '2019-03-31 23:59:59';

-- 647.87 MB
select distinct(affiliated_base_number) from dtc-de-zoom.week_3.table_trip_data_non_partitoned
where pickup_datetime between '2019-03-01 00:00:00' and '2019-03-31 23:59:59';