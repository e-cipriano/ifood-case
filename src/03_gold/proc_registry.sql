%sql
CREATE TABLE IF NOT EXISTS gold.proc_registry (
  proc_name     STRING,
  proc_sql      STRING,
  table_name    STRING,
  instance      STRING,
  data_origins  STRING,
  delta_type    STRING,
  delta_init    DATE,
  delta_end     DATE,
  data_owner    STRING,
  dt_registry   TIMESTAMP 

);

INSERT INTO hive_metastore.gold.proc_registry VALUES
  ('procedure_tbg_yellow_tripdata', 
   'procedure_tbg_yellow_tripdata.sql',
   'tbg_yellow_tripdata',
   'gold',
   'silver.tbs_yellow_tripdata',
   'STATIC DATE RANGE',
   '2023-01-01',
   '2023-05-31', 
   'ifood_tech', 
   current_timestamp()
);