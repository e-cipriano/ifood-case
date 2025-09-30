# ifood-case

## LANDING ZONE
Origem: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
Arquivos Yellow de janeiro a Maio de 2023.

Os documentos foram armazenados no GCP Cloud Storage: [gs://landing_tripdata/yellow_tripdata](https://console.cloud.google.com/storage/browser/landing_tripdata/yellow_tripdata;tab=objects?project=ifood-case-472923&supportedpurview=project&prefix=&forceOnObjectsSortingFiltering=false)

Emissão da chave: [IMA-ADMIN](https://console.cloud.google.com/iam-admin/serviceaccounts/details/118161083804142647688/keys?project=ifood-case-472923&supportedpurview=project)

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
Os dados foram limpos para abrangencia dos camos tpep_pickup_datetime apenas para as datas de 2023-01-01 até 2023-05-31, pois a base tinha outras datas diferentes.
Os dados nulos de passenger_count foram tratados para 1.

Artefatos:
- Notebook DataBricks publicado: [Notebook Silver 2025-09-23](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4018484445800316/2806729363855634/1868833645592944/latest.html)

 src/02_silver/
- procedure_tbs_yellow_tripdata.sql
- create table tbs_yellow_tripdata.sql
- proc_registry.sql


## GOLD ZONE

Os dados da silver-zone foram limpos e selecionados para a camada de consumo (gold-zone).

Artefatos:
- [Notebook Gold 2025-09-24](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4018484445800316/3647327772814116/1868833645592944/latest.html)
- [Notebook Respostas](https://community.cloud.databricks.com/editor/notebooks/3647327772814130?o=4018484445800316#command/3647327772814131)

  src/03_gold
- procedure_tbg_yellow_tripdata.sql
- create table tbg_yellow_tripdata.sql
- proc_registry.sql
- respostas.sql

