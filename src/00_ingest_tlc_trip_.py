# Databricks notebook: 00_ingest_yellow_2023_jan_to_may

from pyspark.sql import functions as F, types as T
import requests, os

# --------- Parâmetros ---------
base_dir = "dbfs:/FileStore/taxi"
landing_dir = f"{base_dir}/landing/yellow/2023"
bronze_dir  = f"{base_dir}/bronze/yellow/2023"
silver_dir  = f"{base_dir}/silver/yellow/2023"
gold_dir    = f"{base_dir}/gold/yellow/2023"

dbutils.fs.mkdirs(landing_dir)
dbutils.fs.mkdirs(bronze_dir)
dbutils.fs.mkdirs(silver_dir)
dbutils.fs.mkdirs(gold_dir)

months = ["01","02","03","04","05"]

# Preencha com os links oficiais de cada mês (Parquet), obtidos na página da TLC
# Ex.: "https://<host-oficial>/trip-data/yellow_tripdata_2023-01.parquet"
# A TLC fornece os links de download mensais nessa página.
parquet_urls = {
    "01": "<URL_OFICIAL_2023_01>",
    "02": "<URL_OFICIAL_2023_02>",
    "03": "<URL_OFICIAL_2023_03>",
    "04": "<URL_OFICIAL_2023_04>",
    "05": "<URL_OFICIAL_2023_05>",
}

# --------- Download para landing ---------
for m in months:
    url = parquet_urls[m]
    local_tmp = f"/tmp/yellow_2023_{m}.parquet"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
    dbutils.fs.mv(f"file:{local_tmp}", f"{landing_dir}/{m}.parquet", True)

# --------- Bronze (raw tabelado) ---------
df_bronze = spark.read.parquet(landing_dir)

# Persistir em Delta particionado por mês (derivado do pickup para navegação)
df_bronze = df_bronze.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(T.TimestampType()))
df_bronze = df_bronze.withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(T.TimestampType()))
df_bronze = df_bronze.withColumn("year", F.year("tpep_pickup_datetime")).withColumn("month", F.date_format("tpep_pickup_datetime", "MM"))

(df_bronze
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year","month")
 .save(bronze_dir))

spark.sql("CREATE DATABASE IF NOT EXISTS taxi")
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS taxi.yellow_bronze
  USING DELTA
  LOCATION '{bronze_dir}'
""")

# --------- Silver (curado com colunas obrigatórias) ---------
# Seleção e limpeza mínima:
# - Garantir colunas: VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime
# - Filtrar registros de 2023-01-01 a 2023-05-31 23:59:59
# - dropoff >= pickup
# - Tipagem consistente
bronze = spark.read.format("delta").load(bronze_dir)

silver = (bronze
    .select(
        F.col("VendorID").cast("int").alias("VendorID"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
    )
    .where(F.col("tpep_pickup_datetime").between("2023-01-01", "2023-05-31 23:59:59"))
    .where(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
    .dropna(subset=["VendorID","passenger_count","total_amount","tpep_pickup_datetime","tpep_dropoff_datetime"])
    # Regras razoáveis (ajuste conforme EDA):
    .where((F.col("passenger_count") >= 0) & (F.col("passenger_count") <= 8))
    .where(F.col("total_amount").isNotNull())  # permitir negativos se houver ajustes/taxas
    .withColumn("year", F.year("tpep_pickup_datetime"))
    .withColumn("month", F.date_format("tpep_pickup_datetime","MM"))
)

(silver
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year","month")
 .save(silver_dir))

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS taxi.yellow_silver
  USING DELTA
  LOCATION '{silver_dir}'
""")

# --------- Gold (ex.: agregações prontas) ---------
# 1) média mensal de total_amount (Jan–Mai/2023)
gold_month_avg = (silver
    .groupBy("year","month")
    .agg(F.avg("total_amount").alias("avg_total_amount"))
)

(gold_month_avg
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year","month")
 .save(f"{gold_dir}/month_avg_total_amount"))

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS taxi.yellow_gold_month_avg_total
  USING DELTA
  LOCATION '{gold_dir}/month_avg_total_amount'
""")

# 2) média de passageiros por hora do dia (somente Maio/2023)
silver_may = silver.where((F.col("year")==2023) & (F.col("month")=="05")) \
                   .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))

gold_may_hour = (silver_may
    .groupBy("year","month","hour_of_day")
    .agg(F.avg("passenger_count").alias("avg_passengers"))
)

(gold_may_hour
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year","month")
 .save(f"{gold_dir}/may_avg_passengers_by_hour"))

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS taxi.yellow_gold_may_avg_passengers_by_hour
  USING DELTA
  LOCATION '{gold_dir}/may_avg_passengers_by_hour'
""")
