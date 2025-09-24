create table gold_zone.tbg_yellow_tripdata (
  VendorID                  INT64       OPTIONS(description="Código do fornecedor"),
  tpep_pickup_datetime      TIMESTAMP   OPTIONS(description="Data e hora de início da viagem"),
  tpep_dropoff_datetime     TIMESTAMP   OPTIONS(description="Data e hora de término da viagem"),
  passenger_count           FLOAT64     OPTIONS(description="Número de passageiros"),
  total_amount              FLOAT64     OPTIONS(description="Valor total da viagem")
)

PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
OPTIONS(
  description="Dados de Viagem de Yellow Taxi em NYC em camada de consumo"
);