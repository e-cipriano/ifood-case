# ifood-case

## LANDING ZONE
Os documentos foram armazenados no GCP Cloud Storage: https://console.cloud.google.com/storage/browser/landing_tripdata/yellow_tripdata;tab=objects?project=ifood-case-472923&supportedpurview=project&prefix=&forceOnObjectsSortingFiltering=false


## BRONZE ZONE
As tabelas foram criadas no GCP BigQuery, 
projeto: ifood-case, 
dataset: bronze_zone
tabelas: yellow_tripdata_2023_01
        yellow_tripdata_2023_02
        yellow_tripdata_2023_03
        yellow_tripdata_2023_04
        yellow_tripdata_2023_05

## SILVER ZONE
Os dados da bronze-zone foram consolidados em uma unica tabela.
projeto: ifood-case,
dataset: silver_zone
tabela: tbs_yellow_tripdata

## GOLD ZONE
Os dados da silver-zone foram limpos e selecionados para a camada de consumo (gold-zone).
dataset: gold_zone
tabela: tbg_yellow_tripdata








