create procedure `silver_zone.prc_tbs_yellow_tripdata` as 

 (
select
  VendorID                  INT64      ,
  tpep_pickup_datetime      TIMESTAMP   ,
  tpep_dropoff_datetime     TIMESTAMP   ,
  passenger_count           FLOAT64     ,
  trip_distance             FLOAT64     ,
  RatecodeID                FLOAT64     ,
  store_and_fwd_flag        STRING      ,
  PULocationID              INT64       ,
  DOLocationID              INT64       ,
  payment_type              INT64       ,
  fare_amount               FLOAT64     ,
  extra                     FLOAT64     ,
  mta_tax                   FLOAT64     ,
  tip_amount                FLOAT64     ,
  tolls_amount              FLOAT64     ,
  improvement_surcharge     FLOAT64     ,
  total_amount              FLOAT64    
  from `bronze_zone.yellow_tripdata_2023_01`

    where tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and passenger_count is not null
        and trip_distance is not null
        and RatecodeID is not null
        and store_and_fwd_flag is not null
        and PULocationID is not null
        and DOLocationID is not null
        and payment_type is not null
        and fare_amount is not null
        and extra is not null
        and mta_tax is not null
        and tip_amount is not null
        and tolls_amount is not null
        and improvement_surcharge is not null
        and total_amount is not null        

        union all

    select
  VendorID                  INT64      ,
  tpep_pickup_datetime      TIMESTAMP   ,
  tpep_dropoff_datetime     TIMESTAMP   ,
  passenger_count           FLOAT64     ,
  trip_distance             FLOAT64     ,
  RatecodeID                FLOAT64     ,
  store_and_fwd_flag        STRING      ,
  PULocationID              INT64       ,
  DOLocationID              INT64       ,
  payment_type              INT64       ,
  fare_amount               FLOAT64     ,
  extra                     FLOAT64     ,
  mta_tax                   FLOAT64     ,
  tip_amount                FLOAT64     ,
  tolls_amount              FLOAT64     ,
  improvement_surcharge     FLOAT64     ,
  total_amount              FLOAT64    
  from `bronze_zone.yellow_tripdata_2023_01`

    where tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and passenger_count is not null
        and trip_distance is not null
        and RatecodeID is not null
        and store_and_fwd_flag is not null
        and PULocationID is not null
        and DOLocationID is not null
        and payment_type is not null
        and fare_amount is not null
        and extra is not null
        and mta_tax is not null
        and tip_amount is not null
        and tolls_amount is not null
        and improvement_surcharge is not null
        and total_amount is not null        

        union all      select
  VendorID                  INT64      ,
  tpep_pickup_datetime      TIMESTAMP   ,
  tpep_dropoff_datetime     TIMESTAMP   ,
  passenger_count           FLOAT64     ,
  trip_distance             FLOAT64     ,
  RatecodeID                FLOAT64     ,
  store_and_fwd_flag        STRING      ,
  PULocationID              INT64       ,
  DOLocationID              INT64       ,
  payment_type              INT64       ,
  fare_amount               FLOAT64     ,
  extra                     FLOAT64     ,
  mta_tax                   FLOAT64     ,
  tip_amount                FLOAT64     ,
  tolls_amount              FLOAT64     ,
  improvement_surcharge     FLOAT64     ,
  total_amount              FLOAT64    
  from `bronze_zone.yellow_tripdata_2023_01`

    where tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and passenger_count is not null
        and trip_distance is not null
        and RatecodeID is not null
        and store_and_fwd_flag is not null
        and PULocationID is not null
        and DOLocationID is not null
        and payment_type is not null
        and fare_amount is not null
        and extra is not null
        and mta_tax is not null
        and tip_amount is not null
        and tolls_amount is not null
        and improvement_surcharge is not null
        and total_amount is not null        

        union all 
select
  VendorID                  INT64      ,
  tpep_pickup_datetime      TIMESTAMP   ,
  tpep_dropoff_datetime     TIMESTAMP   ,
  passenger_count           FLOAT64     ,
  trip_distance             FLOAT64     ,
  RatecodeID                FLOAT64     ,
  store_and_fwd_flag        STRING      ,
  PULocationID              INT64       ,
  DOLocationID              INT64       ,
  payment_type              INT64       ,
  fare_amount               FLOAT64     ,
  extra                     FLOAT64     ,
  mta_tax                   FLOAT64     ,
  tip_amount                FLOAT64     ,
  tolls_amount              FLOAT64     ,
  improvement_surcharge     FLOAT64     ,
  total_amount              FLOAT64    
  from `bronze_zone.yellow_tripdata_2023_01`

    where tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and passenger_count is not null
        and trip_distance is not null
        and RatecodeID is not null
        and store_and_fwd_flag is not null
        and PULocationID is not null
        and DOLocationID is not null
        and payment_type is not null
        and fare_amount is not null
        and extra is not null
        and mta_tax is not null
        and tip_amount is not null
        and tolls_amount is not null
        and improvement_surcharge is not null
        and total_amount is not null        

        union all)
;