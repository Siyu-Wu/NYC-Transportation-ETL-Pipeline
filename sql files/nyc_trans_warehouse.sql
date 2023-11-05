CREATE OR REPLACE DATABASE airflow_data_source;

USE DATABASE airflow_data_source;

CREATE OR REPLACE TABLE citibike (
    ride_id VARCHAR(45),
    rideable_type VARCHAR(45),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration FLOAT,
    start_region INT,
    start_station_name VARCHAR(45),
    start_station_id VARCHAR(45),
    end_region INT,
    end_station_name VARCHAR(45),
    end_station_id VARCHAR(45),
    start_lat FLOAT,
    start_lng FLOAT,
    end_lat FLOAT,
    end_lng FLOAT,
    member_casual VARCHAR(45)
    );

create or replace table zone (
    LocationID int,
    Zone VARCHAR(45)
);

create or replace table weather (
    weather_id int AUTOINCREMENT,
    DATE DATE,
    AWND FLOAT,
    PRCP FLOAT,
    TAVG FLOAT
);

CREATE OR REPLACE TABLE yellow_taxi (
    yellowtaxi_trip_id int AUTOINCREMENT,
    VendorID int,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    duration FLOAT,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR(45),
    PULocationID INT,
    DOLocationID int,
    payment_type int,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT
    );

select * from yellow_taxi;
select * from weather;
select * from zone;
select * from citibike;

