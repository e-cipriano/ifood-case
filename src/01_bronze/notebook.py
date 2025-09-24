%python
#1 Instala as bibliotecas necessárias
%pip install gcsfs
import gcsfs
import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql import SparkSession

#2 Credenciais da conta de serviço (O Databricks Community Edition não estava reconhecendo o arquivo json para recuperar via codigo, o ideal é ocultar e proteger as credenciais.) 
token_dict = {
  
}

#2.2 Inicializa o sistema de arquivos GCS
fs = gcsfs.GCSFileSystem(token=token_dict)

#2.3 Caminho do arquivo Parquet no GCS
gcs_file_path = "landing_tripdata/yellow_tripdata/yellow_tripdata_2023-01.parquet"

#2.4 Lê o arquivo Parquet como pandas
with fs.open(gcs_file_path, 'rb') as f:
    table = pq.read_table(f)
    pdf = table.to_pandas()

#2.5 Converte para Spark DataFrame
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(pdf)

#2.6 Exibe os dados
df.show()

#2.7 Cria uma view temporária
df.createOrReplaceTempView("yellow_tripdata")


#3 Cria a tabela persistente com overwrite (criação inicial)
df.write.mode("overwrite").saveAsTable("yellow_tripdata")
#3.1 Verifica os dados na tabela criada
df.show()

#4 Lista de meses adicionais
meses = ["02", "03", "04", "05"]
#4.1 Captura o schema da tabela persistida
base_schema = spark.table("yellow_tripdata").schema

for mes in meses:
    gcs_file_path = f"landing_tripdata/yellow_tripdata/yellow_tripdata_2023-{mes}.parquet"
    
    #4.1.1 Lê o arquivo Parquet como pandas
    with fs.open(gcs_file_path, 'rb') as f:
        table = pq.read_table(f)
        pdf = table.to_pandas()
    
    #4.1.2 Converte pandas para Spark com schema fixo
    df_mes = spark.createDataFrame(pdf, schema=base_schema)

    
    #4.1.3 Adiciona os dados à tabela existente
    df_mes.write.mode("append").saveAsTable("yellow_tripdata")
    
# 4.2 Verifica os dados na tabela criada
df_final = spark.table("yellow_tripdata")
df_final.show()

# Consulta rápida
spark.sql("SELECT COUNT(*) FROM yellow_tripdata").show()

# Visualiza algumas linhas
spark.sql("SELECT * FROM yellow_tripdata LIMIT 10").show()

