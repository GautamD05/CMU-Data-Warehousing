-- Drop existing tables if they exist
DROP TABLE IF EXISTS yellow_tripdata;
DROP TABLE IF EXISTS fhv_tripdata;
DROP TABLE IF EXISTS fhvhv_tripdata;
DROP TABLE IF EXISTS green_tripdata;
DROP TABLE IF EXISTS citibike_tripdata;
DROP TABLE IF EXISTS central_park_weather;
DROP TABLE IF EXISTS fhv_bases;

create table yellow_tripdata as select * from 
read_parquet('./data/taxi/yellow_tripdata*.parquet',
union_by_name=True, filename=True);

create table fhv_tripdata as select * from 
read_parquet('./data/taxi/fhv_tripdata*.parquet',
union_by_name=True, filename=True);

create table fhvhv_tripdata as select * from 
read_parquet('./data/taxi/fhvhv_tripdata*.parquet',
union_by_name=True, filename=True);

create table green_tripdata as select * from 
read_parquet('./data/taxi/green_tripdata*.parquet',
union_by_name=True, filename=True);

-- Create new tables from CSV files
create table citibike_tripdata as select * from 
read_csv('./data/citibike-tripdata.csv/citibike-tripdata.*',
union_by_name=True, filename=True);

create table central_park_weather as select * from 
read_csv('./data/central_park_weather.*',
union_by_name=True, filename=True);

create table fhv_bases as select * from 
read_csv('./data/fhv_bases.*',
union_by_name=True, filename=True);
