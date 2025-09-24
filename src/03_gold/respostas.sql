%sql 
-- Pergunta: Qual a média de valor total (total_amount) recebido em um mês
-- considerando todos os yellow táxis da frota?

-- 1) Média por mês (média por corrida dentro de cada mês)
SELECT
	date_format(DATE_TRUNC(tpep_pickup_datetime, MONTH),"yyyy-MM") AS month,
	COUNT(1) AS rides_count,
	SUM(total_amount) AS sum_total_amount,
	AVG(total_amount) AS avg_total_amount_per_ride
FROM gold.tbg_yellow_tripdata
GROUP BY month

-- Pergunta: Qual a média de valor total (total_amount) recebido em um mês?
-- considerando todos os yellow táxis da frota?

-- 1) Média por mês (média por corrida dentro de cada mês)
SELECT
  date_format(DATE_TRUNC(tpep_pickup_datetime, MONTH), "yyyy-MM") AS month,
  COUNT(1) AS rides_count,
  SUM(total_amount) AS sum_total_amount,
  AVG(total_amount) AS avg_total_amount_per_ride
FROM gold.tbg_yellow_tripdata
GROUP BY month
ORDER BY month;


-- 2) Média total em um mês específico (template parametrizável)
SELECT
  COUNT(*) AS rides_count,
  SUM(total_amount) AS sum_total_amount,
  AVG(total_amount) AS avg_total_amount_per_ride
FROM gold.tbg_yellow_tripdata
WHERE 
date_format(DATE_TRUNC(tpep_pickup_datetime, MONTH), "yyyy-MM") = “2023-05”


-- 3) Média diária dentro de um mês (útil para comparar dias)
SELECT
  DATE(tpep_pickup_datetime) AS day,
  COUNT(*) AS rides_count,
  SUM(total_amount) AS sum_total_amount,
  AVG(total_amount) AS avg_total_amount_per_ride
FROM gold.tbg_yellow_tripdata
WHERE DATE_TRUNC(tpep_pickup_datetime, MONTH) = DATE_TRUNC(DATE(:year || '-' || LPAD(CAST(:month AS STRING),2,'0')), MONTH)
GROUP BY day
ORDER BY day;


-- 4) Observações / data quality
-- - Recomenda-se filtrar/outlier por valores negativos ou durations inválidas antes de calcular médias.
-- - Exemplo de filtro para remover valores inválidos:
--   WHERE total_amount >= 0
--     AND tpep_dropoff_datetime >= tpep_pickup_datetime

-- Fim