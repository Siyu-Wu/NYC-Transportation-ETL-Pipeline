CREATE OR REPLACE DATABASE docker_nyc_trans_datamart;
USE DATABASE docker_nyc_trans_datamart;

-- DIMENSION TABLE DEFINE
create or replace table date_time (
    time_id DATE PRIMARY KEY
);


create or replace table weather (
    weather_id int PRIMARY KEY,
    date date,
    AWND FLOAT,
    PRCP FLOAT,
    TAVG FLOAT
);

create or replace table zone (
    LocationID int PRIMARY KEY,
    Zone VARCHAR(45)
);

CREATE OR REPLACE TABLE citibike (
    ride_id VARCHAR(45) PRIMARY KEY,
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

CREATE OR REPLACE TABLE yellow_taxi (
    yellowtaxi_trip_id int PRIMARY KEY,
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

CREATE OR REPLACE TABLE trip_fact (
    time_id DATE,
    weather_id int,
    citibike_trip_id VARCHAR(45),
    yellowtaxi_trip_id INT,
    start_locationID INT,
    end_locationID INT,
    duration_diff FLOAT,
constraint PK_TripFact primary key (time_id,weather_id,citibike_trip_id,yellowtaxi_trip_id,start_locationID,end_locationID),
constraint FK_time_id foreign key (time_id) references date_time(time_id),
constraint FK_weather_id foreign key (weather_id) references weather(weather_id),
constraint FK_citibike_trip_id foreign key (citibike_trip_id) references citibike(ride_id),
constraint FK_yellowtaxi_trip_id foreign key (yellowtaxi_trip_id) references yellow_taxi(yellowtaxi_trip_id),
constraint FK_start_locationID foreign key (start_locationID) references zone(LocationID),
constraint FK_end_locationID foreign key (end_locationID) references zone(LocationID)
);




