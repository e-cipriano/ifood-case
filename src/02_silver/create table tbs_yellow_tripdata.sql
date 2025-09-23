create table tbs_yellow_tripdata (
  VendorID                  INT64       OPTIONS(description="Código do fornecedor"),
  tpep_pickup_datetime      TIMESTAMP   OPTIONS(description="Data e hora de início da viagem"),
  tpep_dropoff_datetime     TIMESTAMP   OPTIONS(description="Data e hora de término da viagem"),
  passenger_count           FLOAT64     OPTIONS(description="Número de passageiros"),
  trip_distance             FLOAT64     OPTIONS(description="Distância da viagem em milhas"),
  RatecodeID                FLOAT64     OPTIONS(description="Tipo de tarifa"),
  store_and_fwd_flag        STRING      OPTIONS(description="Indica se a viagem foi armazenada e encaminhada"),
  PULocationID              INT64       OPTIONS(description="Código do local de início da viagem"),
  DOLocationID              INT64       OPTIONS(description="Código do local de término da viagem"),
  payment_type              INT64       OPTIONS(description="Tipo de pagamento"),
  fare_amount               FLOAT64     OPTIONS(description="Valor da tarifa"),
  extra                     FLOAT64     OPTIONS(description="Taxa extra"),
  mta_tax                   FLOAT64     OPTIONS(description="Imposto de Mobilidade para Autoridade de Transporte Metropolitano MTA"),
  tip_amount                FLOAT64     OPTIONS(description="Valor da gorjeta"),
  tolls_amount              FLOAT64     OPTIONS(description="Valor dos pedágios"),
  improvement_surcharge     FLOAT64     OPTIONS(description="Taxa de melhoria"),
  total_amount              FLOAT64    OPTIONS(description="Valor total da viagem")
)

PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
OPTIONS(
  description="Dados de Viagem de Yellow Taxi em NYC"
);