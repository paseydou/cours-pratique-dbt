-- Plan d’Analyse Exploratoire (EDA) - Yellow Taxi NYC

/* 1. Compréhension de la donnée
Objectif : savoir ce qu’on a entre les mains avant de creuser.*/

SELECT * 
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
LIMIT 10;


SELECT
COUNT(*) 
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet';

/* 2. Répartition des principales dimensions
Objectif : comprendre la distribution des variables catégorielles. */

/* Courses par VendorID
Courses par RatecodeID
Courses par payment_type
Répartition du store_and_fwd_flag
Courses par PULocationID et DOLocationID */

-- Courses par VendorID
SELECT 
VendorID,
 COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY VendorID;

-- Courses par RatecodeID
SELECT 
RatecodeID, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY RatecodeID;

-- Répartition du store_and_fwd_flag
SELECT store_and_fwd_flag, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY store_and_fwd_flag;

-- Courses par payment_type 
SELECT payment_type,
 COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY payment_type;

-- Courses par PULocationID
SELECT PULocationID,
 COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY PULocationID;

-- Courses par DOLocationID
SELECT DOLocationID, COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet' 
GROUP BY DOLocationID;

-- 3. Analyse temporelle
 
 SELECT COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE tpep_pickup_datetime > tpep_dropoff_datetime;

SELECT *
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE tpep_pickup_datetime > tpep_dropoff_datetime
LIMIT 10;



 --.read './analyses/analyse_explo.sql'


-- 4. Analyse des valeurs aberrantes et des incohérences
-- Objectif : identifier les valeurs anormales ou incohérentes dans les données.
SELECT COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE trip_distance <= 0;

-- Vérifier les enregistrements avec une distance de trajet négative ou nulle
SELECT tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count, trip_distance
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE trip_distance < 0
LIMIT 10;


-- Vérifier les enregistrements avec une distance de trajet nulle
SELECT tpep_pickup_datetime, tpep_dropoff_datetime,passenger_count,trip_distance
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE trip_distance = 0
LIMIT 10;

-- Vérifier les enregistrements avec un montant total négatif ou nul
SELECT COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE total_amount <= 0;

-- Vérifier les enregistrements avec un montant total négatif
SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, total_amount
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet'
WHERE total_amount < 0
LIMIT 10;