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
SELECT DISTINCT
    TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD')) AS date_id,
    date,
    DAY(date) AS day,
    MONTH(date) AS month,
    YEAR(date) AS year,
    QUARTER(date) AS quarter
FROM dim_date_staging;

SELECT * FROM dim_date;


-- Tabulka dim_time
CREATE OR REPLACE TABLE dim_time AS
WITH minutes AS (
    SELECT
        SEQ4() AS minute_of_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1440))
)
SELECT
    minute_of_day + 1 AS time_id,
    TIME_FROM_PARTS(
        FLOOR(minute_of_day / 60),
        MOD(minute_of_day, 60),
        0
    ) AS time,
    FLOOR(minute_of_day / 60) AS hour,
    MOD(minute_of_day, 60) AS minute,
    CASE
        WHEN FLOOR(minute_of_day / 60) BETWEEN 5 AND 11 THEN 'morning'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END AS part_of_day,
    CASE
        WHEN FLOOR(minute_of_day / 60) BETWEEN 7 AND 9
          OR FLOOR(minute_of_day / 60) BETWEEN 16 AND 18 THEN TRUE
        ELSE FALSE
    END AS is_peak
FROM minutes;

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
    


--Tabulka fact_flight_status
CREATE OR REPLACE TABLE fact_flight_status AS
SELECT
    ROW_NUMBER() OVER (ORDER BY f.ORIGIN_MESSAGE_TIMESTAMP, f.FLIGHT_NUMBER) AS flight_status_id,
    f.FLIGHT_NUMBER,
    fs.flight_state_id,
    a.airline_id,
    dap.airport_id AS departure_airport_id,
    aap.airport_id AS arrival_airport_id,
    dd.date_id AS scheduled_departure_date_id,
    da.date_id AS scheduled_arrival_date_id,
    dt_depart.time_id AS scheduled_departure_time_id,
    dt_arrive.time_id AS scheduled_arrival_time_id,
    f.DEPARTURE_ACTUAL_OUTGATE_LOCAL AS actual_departure_time,
    f.ARRIVAL_ACTUAL_INGATE_LOCAL AS actual_arrival,
    f.SCHEDULED_DEPARTURE_TIME_LOCAL AS scheduled_departure_time,
    f.SCHEDULED_ARRIVAL_TIME_LOCAL AS scheduled_arrival_time,
    f.ACTUAL_TOTAL_SEATS,
    f.PREDICTED_TOTAL_SEATS,
    DATEDIFF('minute', f.SCHEDULED_DEPARTURE_TIME_LOCAL, f.DEPARTURE_ACTUAL_OUTGATE_LOCAL) AS departure_delay_minutes,
    DATEDIFF('minute', f.SCHEDULED_ARRIVAL_TIME_LOCAL, f.ARRIVAL_ACTUAL_INGATE_LOCAL) AS arrival_delay_minutes
FROM FLIGHT_STATUS_LATEST_STAGING f
LEFT JOIN dim_flight_state fs ON f.FLIGHT_STATE = fs.FLIGHT_STATE
LEFT JOIN dim_airline a ON f.IATA_CARRIER_CODE = a.IATA_CARRIER_CODE
LEFT JOIN dim_airport dap ON f.DEPARTURE_IATA_AIRPORT_CODE = dap.IATA_AIRPORT_CODE
LEFT JOIN dim_airport aap ON f.ARRIVAL_IATA_AIRPORT_CODE = aap.IATA_AIRPORT_CODE
LEFT JOIN dim_date dd ON TO_NUMBER(TO_VARCHAR(f.SCHEDULED_DEPARTURE_DATE_LOCAL, 'YYYYMMDD')) = dd.date_id
LEFT JOIN dim_date da ON TO_NUMBER(TO_VARCHAR(f.SCHEDULED_ARRIVAL_TIME_LOCAL::DATE, 'YYYYMMDD')) = da.date_id
LEFT JOIN dim_time dt_depart ON TO_TIME(f.SCHEDULED_DEPARTURE_TIME_LOCAL) = dt_depart.time
LEFT JOIN dim_time dt_arrive ON TO_TIME(f.SCHEDULED_ARRIVAL_TIME_LOCAL) = dt_arrive.time;

SELECT * FROM fact_flight_status;
