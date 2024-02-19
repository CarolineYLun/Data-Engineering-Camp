-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `formal-incline-413216.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-01.parquet', 
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-02.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-03.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-04.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-05.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-06.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-07.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-08.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-09.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-10.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-11.parquet',
          'gs://mage-zoomcamp-lun/green_tripdata_2022/green_tripdata_2022-12.parquet']
);

--Check green trip data
SELECT *FROM `formal-incline-413216.ny_taxi.external_green_tripdata` limit 10;

-- Create a non partitioned table from external table [copy from google cloud storage to bigquery storage ]
CREATE OR REPLACE TABLE `formal-incline-413216.ny_taxi.green_tripdata_non_partitioned` AS
SELECT * FROM `formal-incline-413216.ny_taxi.external_green_tripdata`;

--Q2:
-- Scanned 0B
SELECT COUNT(DISTINCT(PULocationID)) as NumLocationIDs
FROM formal-incline-413216.ny_taxi.external_green_tripdata;

-- Scanned 6.41MB
SELECT COUNT(DISTINCT(PULocationID)) AS NumLocationIDs
FROM formal-incline-413216.ny_taxi.green_tripdata_non_partitioned;

--Q3: 
SELECT COUNT(*) AS NumFareAmount
FROM formal-incline-413216.ny_taxi.green_tripdata_non_partitioned
WHERE fare_amount=0;

--Q4:
-- Creating a partition and cluster table
-- Partitioning divides a table into segments, while clustering sorts the table based on user-defined columns.
CREATE OR REPLACE TABLE `formal-incline-413216.ny_taxi.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime) -- filter based on pickup
CLUSTER BY PUlocationID AS -- use cluster to order/sort the result 
SELECT * FROM `formal-incline-413216.ny_taxi.external_green_tripdata`;

--Q5:
-- Query scans 12.82 MB for non-partitioned table
SELECT DISTINCT PULocationID as trips
FROM formal-incline-413216.ny_taxi.green_tripdata_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Query scans 1.12 MB for partitioned and clustered table
SELECT DISTINCT PULocationID as trips
FROM formal-incline-413216.ny_taxi.green_tripdata_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


--Q8:
-- Scanned 114.11MB
SELECT * FROM formal-incline-413216.ny_taxi.green_tripdata_non_partitioned;