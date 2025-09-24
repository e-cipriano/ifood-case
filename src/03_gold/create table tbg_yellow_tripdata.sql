CREATE TABLE IF NOT EXISTS gold.tbg_yellow_tripdata (
  VendorID                  INT64       ,
  tpep_pickup_datetime      TIMESTAMP   ,
  tpep_dropoff_datetime     TIMESTAMP   ,
  passenger_count           FLOAT64     ,
  total_amount              FLOAT64    ,
  tpep_pickup_hour        INT64

PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID

