create table yellow_tripdata (
  VendorID                  INT64       ,
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
)

PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID

