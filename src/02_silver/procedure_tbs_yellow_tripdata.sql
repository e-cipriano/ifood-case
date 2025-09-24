%sql
-- Criando schema Silver, caso ela ainda não exista (no ambiente de testes pode desaparecer):
CREATE SCHEMA IF NOT EXISTS hive_metastore.silver;

-- Criando a tabela Silver, caso ela ainda não exista (no ambiente de testes pode desaparecer):
CREATE TABLE IF NOT EXISTS hive_metastore.silver.tbs_yellow_tripdata (
  VendorID                INT,
  tpep_pickup_datetime    TIMESTAMP,
  tpep_dropoff_datetime   TIMESTAMP,
  passenger_count         INT,
  trip_distance           DOUBLE,
  RatecodeID              INT,
  store_and_fwd_flag      STRING,
  PULocationID            INT,
  DOLocationID            INT,
  payment_type            INT,
  fare_amount             DOUBLE,
  extra                   DOUBLE,
  mta_tax                 DOUBLE,
  tip_amount              DOUBLE,
  tolls_amount            DOUBLE,
  improvement_surcharge   DOUBLE,
  total_amount            DOUBLE,
  congestion_surcharge    DOUBLE
);

-- criando uma CTE para tratamento e processamento de dados utilizando um delta (para casos de produçao e dados mais atuais)

CREATE TEMP VIEW yellow_tripdata as (
SELECT
  CAST(VendorID               AS INT)       AS VendorID,
  CAST(tpep_pickup_datetime   AS TIMESTAMP) AS tpep_pickup_datetime,
  CAST(tpep_dropoff_datetime  AS TIMESTAMP) AS tpep_dropoff_datetime,
  CAST(passenger_count        AS INT)       AS passenger_count,
  CAST(trip_distance          AS DOUBLE)    AS trip_distance,
  CAST(RatecodeID             AS INT)       AS RatecodeID,
  CAST(store_and_fwd_flag     AS STRING)    AS store_and_fwd_flag,
  CAST(PULocationID           AS INT)       AS PULocationID,
  CAST(DOLocationID           AS INT)       AS DOLocationID,
  CAST(payment_type           AS INT)       AS payment_type,
  CAST(fare_amount            AS DOUBLE)    AS fare_amount,
  CAST(extra                  AS DOUBLE)    AS extra,
  CAST(mta_tax                AS DOUBLE)    AS mta_tax,
  CAST(tip_amount             AS DOUBLE)    AS tip_amount,
  CAST(tolls_amount           AS DOUBLE)    AS tolls_amount,
  CAST(improvement_surcharge  AS DOUBLE)    AS improvement_surcharge,
  CAST(total_amount           AS DOUBLE)    AS total_amount,
  CAST(congestion_surcharge   AS DOUBLE)    AS congestion_surcharge
FROM hive_metastore.default.yellow_tripdata
WHERE VendorID               IS NOT NULL
  AND tpep_pickup_datetime   IS NOT NULL
  AND tpep_dropoff_datetime  IS NOT NULL
  AND passenger_count        IS NOT NULL
  AND total_amount           IS NOT NULL
  AND date_format(tpep_pickup_datetime, "yyyy-MM-dd")
      BETWEEN date('2023-01-01') -- Use delta inicio "date_sub(current_date(), 60)"
          AND date('2023-05-31') -- Use delta final  "date_sub(current_date(), 1)"
);


-- Realizando 'upsert' update + insert na tabela final utilizando o CTE tratado:

MERGE INTO silver.tbs_yellow_tripdata AS target
USING yellow_tripdata AS source
  ON  target.VendorID              = source.VendorID
  AND target.tpep_pickup_datetime  = source.tpep_pickup_datetime
  AND target.tpep_dropoff_datetime = source.tpep_dropoff_datetime

WHEN MATCHED THEN
  UPDATE SET
    passenger_count        = source.passenger_count,
    trip_distance          = source.trip_distance,
    RatecodeID             = source.RatecodeID,
    store_and_fwd_flag     = source.store_and_fwd_flag,
    PULocationID           = source.PULocationID,
    DOLocationID           = source.DOLocationID,
    payment_type           = source.payment_type,
    fare_amount            = source.fare_amount,
    extra                  = source.extra,
    mta_tax                = source.mta_tax,
    tip_amount             = source.tip_amount,
    tolls_amount           = source.tolls_amount,
    improvement_surcharge  = source.improvement_surcharge,
    total_amount           = source.total_amount,
    congestion_surcharge   = source.congestion_surcharge

WHEN NOT MATCHED THEN
  INSERT (
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
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
    congestion_surcharge
  )
  VALUES (
    source.VendorID,
    source.tpep_pickup_datetime,
    source.tpep_dropoff_datetime,
    source.passenger_count,
    source.trip_distance,
    source.RatecodeID,
    source.store_and_fwd_flag,
    source.PULocationID,
    source.DOLocationID,
    source.payment_type,
    source.fare_amount,
    source.extra,
    source.mta_tax,
    source.tip_amount,
    source.tolls_amount,
    source.improvement_surcharge,
    source.total_amount,
    source.congestion_surcharge
  );
