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
