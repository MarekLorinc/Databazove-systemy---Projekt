--Priemerné meškanie podľa leteckej spoločnosti
SELECT
    a.IATA_CARRIER_CODE,
     AVG(f.arrival_delay_minutes) AS avg_arrival_delay
FROM fact_flight_status f
JOIN dim_airline a ON f.airline_id = a.airline_id
WHERE f.arrival_delay_minutes > 0
GROUP BY a.IATA_CARRIER_CODE
ORDER BY avg_arrival_delay DESC;

--Počet letov podľa stavu letu
SELECT
    fs.FLIGHT_STATE,
    COUNT(*) AS flight_count
FROM fact_flight_status f
JOIN dim_flight_state fs
    ON f.flight_state_id = fs.flight_state_id
GROUP BY fs.FLIGHT_STATE
ORDER BY flight_count DESC;

--Počet letov podľa časti dňa
SELECT
    t.part_of_day,
    COUNT(*) AS flight_count
FROM fact_flight_status f
JOIN dim_time t
  ON f.scheduled_departure_time_id = t.time_id
GROUP BY t.part_of_day
ORDER BY flight_count DESC;

--Porovnanie skutočných vs. predikovaných sedadiel
SELECT
    a.IATA_CARRIER_CODE,
    AVG(f.ACTUAL_TOTAL_SEATS) AS avg_actual_seats,
    AVG(f.PREDICTED_TOTAL_SEATS) AS avg_predicted_seats
FROM fact_flight_status f
JOIN dim_airline a ON f.airline_id = a.airline_id
GROUP BY a.IATA_CARRIER_CODE
ORDER BY avg_actual_seats DESC;

--Top 10 destinácií podľa počtu letov
SELECT
    ap.COUNTRY_CODE AS destination_country,
    COUNT(*) AS number_of_flights
FROM fact_flight_status f
JOIN dim_airport ap
    ON f.arrival_airport_id = ap.airport_id
WHERE ap.COUNTRY_CODE IS NOT NULL
GROUP BY ap.COUNTRY_CODE
ORDER BY number_of_flights DESC
LIMIT 10;

--Priemerné meškanie odletov počas dňa (v minútach)
SELECT
    t.hour,
    AVG(f.departure_delay_minutes) AS avg_departure_delay
FROM fact_flight_status f
JOIN dim_time t ON f.scheduled_departure_time_id = t.time_id
GROUP BY t.hour
ORDER BY t.hour;

--Top 10 destinácií podľa počtu letov
SELECT
    ap.COUNTRY_CODE AS destination_country,
    COUNT(*) AS number_of_flights
FROM fact_flight_status f
JOIN dim_airport ap
    ON f.arrival_airport_id = ap.airport_id
WHERE ap.COUNTRY_CODE IS NOT NULL
GROUP BY ap.COUNTRY_CODE
ORDER BY number_of_flights DESC
LIMIT 10;
