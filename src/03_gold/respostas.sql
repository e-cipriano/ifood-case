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

-- 1) Média por mês ( por corrida dentro de cada mês)
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

-- Media de passageiros por hora do dia, no mes de maio de 2023
SELECT
    tpep_pickup_hour,
    AVG(passenger_count) AS avg_passenger_count
FROM gold.tbg_yellow_tripdata0
WHERE 
    date_format(tpep_pickup_datetime, "yyyy-MM") = "2023-05"
GROUP BY tpep_pickup_hour
ORDER BY tpep_pickup_hour;  

-- Fim