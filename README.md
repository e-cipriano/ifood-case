# ifood-case

## LANDING ZONE
Origem: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
Arquivos Yellow de janeiro a Maio de 2023.

Os documentos foram armazenados no GCP Cloud Storage: [gs://landing_tripdata/yellow_tripdata](https://console.cloud.google.com/storage/browser/landing_tripdata/yellow_tripdata;tab=objects?project=ifood-case-472923&supportedpurview=project&prefix=&forceOnObjectsSortingFiltering=false)

## BRONZE ZONE
O notebook.py é o arquivo de execução de código no Databricks para ELT e gravação dos dados em tabela. Use um notebook e cole o código para a correta execução. Também disponível no databricks publicamente: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4018484445800316/2896706219050184/1868833645592944/latest.html

Artefatos:
src/01_bronze/
- notebook.py
- create table yellow_tripdata.sql

A tabela foi criada no Databricks em hive_metastore/default/yellow_tripdata , usando as

Tabelas origem:
- yellow_tripdata_2023_01, 
- yellow_tripdata_2023_02, 
- yellow_tripdata_2023_03, 
- yellow_tripdata_2023_04, 
- yellow_tripdata_2023_05 

## SILVER ZONE

Artefatos:
src/02_silver/
- create prc_tbs_yellow_tripdata.sql
- create table tbs_yellow_tripdata.sql


## GOLD ZONE
em construção
Os dados da silver-zone foram limpos e selecionados para a camada de consumo (gold-zone).









