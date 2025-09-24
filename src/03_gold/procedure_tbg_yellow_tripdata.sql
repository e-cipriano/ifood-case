
%sql
-- Criando schema GOLD, caso ela ainda não exista (no ambiente de testes pode desaparecer):
CREATE SCHEMA IF NOT EXISTS hive_metastore.gold;

-- Criando a tabela Silver, caso ela ainda não exista (no ambiente de testes pode desaparecer):
CREATE TABLE IF NOT EXISTS hive_metastore.gold.tbg_yellow_tripdata (
  VendorID                INT,
  tpep_pickup_datetime    TIMESTAMP,
  tpep_dropoff_datetime   TIMESTAMP,
  passenger_count         INT,
  total_amount            DOUBLE
);

-- criando uma CTE para tratamento e processamento de dados utilizando um delta (para casos de produção e dados mais atuais)

CREATE TEMP VIEW temp_yellow_tripdata as (
SELECT
  CAST(VendorID               AS INT)       AS VendorID,
  CAST(tpep_pickup_datetime   AS TIMESTAMP) AS tpep_pickup_datetime,
  CAST(tpep_dropoff_datetime  AS TIMESTAMP) AS tpep_dropoff_datetime,
  CAST(passenger_count        AS INT)       AS passenger_count,
  CAST(total_amount           AS DOUBLE)    AS total_amount
  
FROM hive_metastore.silver.tbs_yellow_tripdata
);


-- Realizando 'upsert' update + insert na tabela final utilizando o CTE tratado:

MERGE INTO gold.tbg_yellow_tripdata AS target
USING temp_yellow_tripdata AS source
  ON  target.VendorID              = source.VendorID
  AND target.tpep_pickup_datetime  = source.tpep_pickup_datetime
  AND target.tpep_dropoff_datetime = source.tpep_dropoff_datetime

WHEN MATCHED THEN
  UPDATE SET
    passenger_count        = source.passenger_count,
    total_amount           = source.total_amount

WHEN NOT MATCHED THEN
  INSERT (
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    total_amount
  )
  VALUES (
    source.VendorID,
    source.tpep_pickup_datetime,
    source.tpep_dropoff_datetime,
    source.passenger_count,
    source.total_amount
  );
