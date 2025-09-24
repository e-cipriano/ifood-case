%python
#1 Instala as bibliotecas necessárias
%pip install gcsfs
import gcsfs
import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql import SparkSession

#2 Credenciais da conta de serviço (O Databricks Community Edition não estava reconhecendo o arquivo json para recuperar via codigo, o ideal é ocultar e proteger as credenciais.) 
token_dict = {
  "type": "service_account",
  "project_id": "ifood-case-472923",
  "private_key_id": "6803c1690aab325203bb03411da404fe6b464499",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDN7wMp9V/O5Evc\npHuASMRhDX2z+xbuxZR2tQbWpIEU2RG4P6HTaj1fFu/kvvQCUwx113TaWI3G7x0l\nCC7Mt7O+wZIGjIonWhEIkOdSP4/0OHjfy6VyWtxBux71QpStg+X3tL7LjjxJqbJw\nOWhKf/+LImZa//ha/vVn3BffablxdCY7HxU5QTKk40QL5mzOTaA6bsvJ85NvxK+3\nkobnUS2UsTWItI5aaT3beMCaABFDEPYsxM1QQJI6IjEaL9wt7+7jWbV+CNy4m73A\n0cqsCkNjYmilKuv0oAjjF7d6KXpVH26oYqhF4ImdLbTnsFA/sOL4WRAFppYF4wPR\nt8ENT8FhAgMBAAECggEADs6tKJAx17uHT5HYJizqn0aMD6vx8IXucJarAOC1Pggv\nA6jiUZmlrg0FKip80HSjmbmTTRE+/ds6+kBbX8b5urFKdwQV+acP6yYYK/bRZVsb\nfeQJEm7sFXcx+P//lsu8IRKIVt9JqyDzk/7cu+FLlk0z6j6A33Ijt9hrMwT0G5nV\nt+gQUVjzqn64cq801lWzbshX7KR/4Vm6DW7/yuFHdxNLE2SJJztTkm6fssyxCQbj\nOowQ4r/o05KRlgXZzk8fbjsbRBQMiOJgXJgRpLCJpBg/Y4RYIOc2Px1WUnz9TJRX\nLgHEScO0UUECM20F6Gh5rcLlQHCnH9/ZQAdZKfK4/QKBgQDtqC1ynj+j8il+F84g\naAf5EkP5FzYFyxfn8zboLWxmIiGNLNi0Pi+cnjV9zBMW0gFD3tFRDqcICKz7iC9Z\n9Vmg66RoAYdfJiE/n3T+B2yHJ4AZ60+W6r7RVeuWHzgTZvu3XnEWWcR58cFO5tsA\nHqg9Shxo/u0vsQDJCHoKJak2owKBgQDd1AUEZ8Mw49ChGUpbtUAR70OvD7kEEB2u\n4JFxwEzZGiaC7W+kYHfX3iDLGi10HCL32pDXyWN0YemxU/TkbtNz+eyrbFqh8AzL\nzKCjmqotDUK6KJGPK+xhvqGYNfwUXFlwW2c0eL2e8khNNQvIi5mCs0IMTpohwyE4\n7XY4fctcKwKBgH9kWQhJn5+IHYnDxPBGE5AFpH4PXRv549Sn4NTQFH0i6o08buUf\nOHJhtBa8n7bp11fERruGeS6rR96E/6zfAa5q4fQGbcQpMkVieln5LMm9+MLsCfvB\n1Yts3R3Zmjt8Ro3iiAgNEm6zkoVy5g3IYTKJWXVGwWmBHgSlEJYD3hK1AoGBALRd\nWwL892l+IrmKXbp5gjHS8J6b0xypmekCMWBjMljn+V5FfpwRz//WXaWmkESzR/9t\nWI4L8nDD2AbWM+206vuGv9eWT48SJuViaU79R9c/y/yfVdMqrJXBMZUYd/F6MUK5\nHw48Cjn916nnMsCKOXRbGp31Hz/Kb+3grlo6dipnAoGABV8BZi4LSh9VQ5pcFoDr\nJ1TdN33hO+cAGWNUsmpIuyRbIERTRrQMcV70cz0gtjbzXSdw9CuyWTrIKPixVN62\n/NlKjwU9MiBrnCjm+MQixdiJCl4Ef7W6X0dFOcfPfDBLuwnMhMSBmmPMU6mJ8N6k\nFtmL8WcH33AxzBxda/kq7zQ=\n-----END PRIVATE KEY-----\n",
  "client_email": "849461728303-compute@developer.gserviceaccount.com",
  "client_id": "118161083804142647688",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/849461728303-compute%40developer.gserviceaccount.com",
  "universe_domain": "googleapis.com"
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

