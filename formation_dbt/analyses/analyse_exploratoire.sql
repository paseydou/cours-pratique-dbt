--- voir un petit aperçu des données de données brutes
SELECT *
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
LIMIT 10;


-- Volume total
SELECT COUNT(*) AS total_trips
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';


-- Vérifier la repartition des trajets dans certaines colonnes

-- Répartition des trajets par fournisseur
SELECT VENDORID, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY VENDORID;


Distribution horaire des départs
SELECT 
  EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
  COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Répartition des trajets par code de tarif
SELECT RATECODEID,
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY RATECODEID;


-- Répartition par type de paiement
SELECT payment_type, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY payment_type;



-- Nombre de trajets par indicateur de stockage et de transfert
SELECT store_and_fwd_flag, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY store_and_fwd_flag;

-- Nombre de trajets par heure de départ
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY pickup_hour;

-- Trajets par heure d'arrivée
SELECT PulocationID, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY PulocationID;

-- Trajets par zone de départ 
SELECT DolocationID, 
COUNT(*) AS trips_count
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY DolocationID;