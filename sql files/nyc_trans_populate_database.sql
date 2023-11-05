use DATABASE airflow_data_source;


insert into docker_nyc_trans_datamart.public.date_time(
    time_id
)
select distinct DATE from weather;


insert into docker_nyc_trans_datamart.public.weather(
    weather_id,
    date,
    AWND,
    PRCP,
    TAVG
)
select distinct weather_id, date,AWND,PRCP,TAVG from weather;

insert into docker_nyc_trans_datamart.public.zone(
    LocationID,
    Zone
)
select distinct LocationID,Zone from zone;


insert into docker_nyc_trans_datamart.public.citibike(
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    duration,
    start_region,
    start_station_name,
    start_station_id,
    end_region,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual
)
select distinct ride_id,rideable_type,started_at,ended_at,duration,start_region,start_station_name,start_station_id,end_region,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual from citibike;

insert into docker_nyc_trans_datamart.public.yellow_taxi(
    yellowtaxi_trip_id,
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    duration,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
)
select distinct yellowtaxi_trip_id,VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    duration,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
from yellow_taxi;


insert into docker_nyc_trans_datamart.public.trip_fact(
    time_id,
    weather_id,
    citibike_trip_id,
    yellowtaxi_trip_id,
    start_locationID,
    end_locationID,
    duration_diff
)

select distinct w.date time_id, w.weather_id weather_id, c.ride_id, y.yellowtaxi_trip_id, c.start_region, c.end_region, (c.duration-y.duration) as duration_diff
from citibike c
inner join yellow_taxi y
on c.start_region = y.pulocationid
and c.end_region = y.dolocationid
inner join weather w
on DATE(c.started_at) = w.date
;

-- select distinct w.date time_id, w.weather_id weather_id, c.ride_id, y.yellowtaxi_trip_id, c.start_region, c.end_region, (c.duration-y.duration) as duration_diff
-- from citibike c
-- inner join yellow_taxi y
-- on c.start_region = y.pulocationid
-- and c.end_region = y.dolocationid
-- inner join weather w
-- on DATE(c.started_at) = w.date
-- ;

-- select * from trip_fact;

