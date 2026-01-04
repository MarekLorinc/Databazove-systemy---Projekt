USE warehouse MONGOOSE_WH;
USE database MONGOOSE_DB;

-- Vytvorenie schemy
CREATE schema projekt_schema;
USE schema projekt_schema;

-- Vytvorenie staging tabulky
CREATE OR REPLACE TABLE FLIGHT_STATUS_LATEST_staging AS
SELECT *
FROM OAG_FLIGHT_STATUS_DATA_SAMPLE.PUBLIC.FLIGHT_STATUS_LATEST_SAMPLE;

-- Výpis prvých 10 riadkov na overenie
SELECT * FROM FLIGHT_STATUS_LATEST_staging LIMIT 10;



--LOAD
-- Tabulka dim_airline staging
CREATE OR REPLACE TABLE dim_airline_staging AS
SELECT 
    IATA_CARRIER_CODE,
    ICAO_CARRIER_CODE
FROM FLIGHT_STATUS_LATEST_STAGING
WHERE IATA_CARRIER_CODE IS NOT NULL;

SELECT * FROM dim_airline_staging;


-- Tabulka dim_airport_staging
CREATE OR REPLACE TABLE dim_airport_staging AS
    SELECT DEPARTURE_IATA_AIRPORT_CODE AS IATA_AIRPORT_CODE
FROM FLIGHT_STATUS_LATEST_STAGING
UNION ALL
    SELECT ARRIVAL_IATA_AIRPORT_CODE
FROM FLIGHT_STATUS_LATEST_STAGING;

SELECT * FROM dim_airport_staging;


-- Tabulka dim_date staging
CREATE OR REPLACE TABLE dim_date_staging AS
    SELECT DISTINCT
        SCHEDULED_DEPARTURE_DATE_LOCAL AS date
FROM FLIGHT_STATUS_LATEST_STAGING
WHERE SCHEDULED_DEPARTURE_DATE_LOCAL IS NOT NULL;

SELECT * FROM dim_date_staging;


-- Tabulka dim_time_staging
CREATE OR REPLACE TABLE dim_time_staging AS
    SELECT SCHEDULED_DEPARTURE_TIME_LOCAL AS time_val
FROM FLIGHT_STATUS_LATEST_STAGING
WHERE SCHEDULED_DEPARTURE_TIME_LOCAL IS NOT NULL
UNION ALL
    SELECT SCHEDULED_ARRIVAL_TIME_LOCAL
FROM FLIGHT_STATUS_LATEST_STAGING
WHERE SCHEDULED_ARRIVAL_TIME_LOCAL IS NOT NULL;

SELECT * FROM dim_time_staging;


-- Tabulka dim_flight_state_staging
CREATE OR REPLACE TABLE dim_flight_state_staging AS
SELECT FLIGHT_STATE
FROM FLIGHT_STATUS_LATEST_STAGING;

SELECT * FROM dim_flight_state_staging;



--TRANSFORM
-- Transform dim_airline - deduplikácia a surrogate key
CREATE OR REPLACE TABLE dim_airline AS
WITH unique_airlines AS (
    SELECT DISTINCT IATA_CARRIER_CODE, ICAO_CARRIER_CODE
    FROM dim_airline_staging
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY IATA_CARRIER_CODE) AS airline_id,
    IATA_CARRIER_CODE,
    ICAO_CARRIER_CODE
FROM unique_airlines;

SELECT * FROM dim_airline;


-- Transform dim_airport - deduplikácia a surrogate key
CREATE OR REPLACE TABLE dim_airport AS
WITH unique_airports AS (
    SELECT DISTINCT IATA_AIRPORT_CODE
    FROM dim_airport_staging
    WHERE IATA_AIRPORT_CODE IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY IATA_AIRPORT_CODE) AS airport_id,
    IATA_AIRPORT_CODE
FROM unique_airports;

SELECT * FROM dim_airport;


-- Transform dim_date - pridanie dátových atribútov + surrogate key
CREATE OR REPLACE TABLE dim_date AS
SELECT
    TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD')) AS date_id,
    date,
    DAY(date) AS day,
    MONTH(date) AS month,
    YEAR(date) AS year,
    QUARTER(date) AS quarter
FROM dim_date_staging;

SELECT * FROM dim_date;


-- Transform dim_time - deduplikácia, surrogate key, výpočty časových segmentov
CREATE OR REPLACE TABLE dim_time AS
WITH unique_times AS (
    SELECT DISTINCT time_val
    FROM dim_time_staging
    WHERE time_val IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY time_val) AS time_id,
    TO_TIME(time_val) AS time,
    EXTRACT(hour FROM time_val) AS hour,
    EXTRACT(minute FROM time_val) AS minute,
    CASE
        WHEN EXTRACT(hour FROM time_val) BETWEEN 5 AND 11 THEN 'morning'
        WHEN EXTRACT(hour FROM time_val) BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN EXTRACT(hour FROM time_val) BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END AS part_of_day,
    CASE
        WHEN EXTRACT(hour FROM time_val) BETWEEN 7 AND 9
          OR EXTRACT(hour FROM time_val) BETWEEN 16 AND 18 THEN TRUE
        ELSE FALSE
    END AS is_peak
FROM unique_times;

SELECT * FROM dim_time;


-- Transform dim_flight_state - deduplikácia a surrogate key
CREATE OR REPLACE TABLE dim_flight_state AS
WITH unique_states AS (
    SELECT DISTINCT FLIGHT_STATE
    FROM dim_flight_state_staging
)
SELECT
    ROW_NUMBER() OVER (ORDER BY FLIGHT_STATE) AS flight_state_id,
    FLIGHT_STATE
FROM unique_states;

SELECT * FROM dim_flight_state;
