# ELT proces pre dataset FLIGHT_STATUS_LATEST_SAMPLE

---

## 1. Úvod a popis zdrojových dát

Dataset FLIGHT_STATUS_LATEST_SAMPLE pochádza zo Snowflake Marketplace. Tento dataset obsahuje podrobné informácie o aktuálnom stave letov, vrátane plánovaných a skutočných časov odletov a príletov, kódov letísk, typu letu, počtu sedadiel a ďalších atribútov.

Cieľom analýzy je vytvoriť dátový model, ktorý umožní sledovať a analyzovať výkonnosť letov, napríklad meškania, predpovede kapacít, či iné metriky súvisiace s prevádzkou letov.

Dataset obsahuje jednu hlavnú tabuľku so 73 stĺpcami, ktoré pokrývajú rôzne aspekty letov, ako sú informácie o leteckej spoločnosti, letisku odletu a príletu, plánované a skutočné časy a ďalšie.

---Image here

Pre účely projektu boli zvolené relevantné atribúty na vytvorenie dimenzionálneho modelu a ELT procesu.

---

## 2. Návrh dimenzionálneho modelu

Pre efektívnu analýzu a vizualizáciu dát bol navrhnutý hviezdicový (Star Schema) dátový model pozostávajúci z jednej faktovej tabuľky a viacerých dimenzií:

---Image here

- **Faktová tabuľka `fact_flight_status`** obsahuje metriky ako skutočné a plánované časy, počet sedadiel a meškania.
- **Dimenzia `dim_airline`** s informáciami o leteckej spoločnosti (kódy IATA_CARRIER, ICAO_CARRIER).
- **Dimenzia `dim_airport`** obsahuje údaje o letiskách odletu a príletu (kódy IATA_AIRPORT, ICAO_AIRPORT, krajiny).
- **Dimenzia `dim_date`** pre plánované dátumy odletov a príletov (deň, mesiac, rok, štvrťrok).
- **Dimenzia `dim_time`** pre časové údaje odletov a príletov (hodina, minúta, časť dňa).

Každá dimenzia poskytuje kontext k faktovým údajom a umožňuje multidimenzionálnu analýzu letových údajov, napríklad podľa času, leteckej spoločnosti alebo letiska.

---

## 3. ELT proces

ELT proces pozostáva z troch hlavných fáz:
- **Extract** – získanie dát zo Snowflake Marketplace
- **Load** – načítanie dát do staging tabuliek
- **Transform** – čistenie, deduplikácia a tvorba finálneho dimenzionálneho modelu

---

### 3.1 Extract

Pre extrakciu bola vytvorená staging tabuľka vo vlastnej databáze a schéme, kde boli vložené všetky stĺpce z pôvodnej tabuľky pre ďalšiu transformáciu a spracovanie.

SQL príkaz pre extrakciu a vytvorenie staging tabuľky:

```sql
CREATE OR REPLACE TABLE FLIGHT_STATUS_LATEST_staging AS
SELECT *
FROM OAG_FLIGHT_STATUS_DATA_SAMPLE.PUBLIC.FLIGHT_STATUS_LATEST_SAMPLE;
```

Staging tabuľka umožňuje pracovať s dátami bez priameho zásahu do zdrojového datasetu a slúži ako základ pre ďalšie spracovanie.

---

### 3.2 Load

Vo fáze **Load** boli dáta zo staging tabuľky rozdelené do ďalších staging tabuliek, z ktorých každá zodpovedá jednej dimenzii v navrhnutom dimenzionálnom modeli.

V tejto fáze bolo cieľom iba logické rozdelenie dát podľa dimenzií.

Vytvorené staging tabuľky:
- `dim_airline_staging`
- `dim_airport_staging`
- `dim_date_staging`
- `dim_flight_state_staging`

SQL príkaz pre načítanie dát do staging tabuľky pre letecké spoločnosti:

```sql
CREATE OR REPLACE TABLE dim_airline_staging AS
SELECT 
    IATA_CARRIER_CODE,
    ICAO_CARRIER_CODE
FROM FLIGHT_STATUS_LATEST_STAGING
WHERE IATA_CARRIER_CODE IS NOT NULL;
```

Tento prístup umožňuje vykonávať všetky transformácie až nad dátami uloženými v databáze Snowflake, čo zjednodušuje správu procesu a zvyšuje jeho flexibilitu.

---

### 3.3 Transform

Vo fáze Transform boli dáta zo staging tabuliek:

- `deduplikované`
- `obohatené o odvodené atribúty`
- `vybavené surrogate keys`
- `uložené do finálnych dimenzií a faktovej tabuľky`

#### Dimenzia dim_airline

Dimenzia obsahuje unikátne letecké spoločnosti identifikované pomocou IATA a ICAO kódov. Pre každý záznam bol vytvorený surrogate key. 
Dimenzia je navrhnutá ako SCD Typ 0, keďže identifikátory leteckých spoločností sa v čase nemenia.

```sql
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
```

#### Dimenzia dim_airport

Dimenzia obsahuje zoznam unikátnych letísk získaných zo stĺpcov letísk odletu a príletu. Každé letisko je reprezentované pomocou IATA kódu a surrogate key. 
Dimenzia je nemenná a klasifikovaná ako SCD Typ 0.

```sql
CREATE OR REPLACE TABLE dim_airport AS
WITH unique_airports AS (
    SELECT
        IATA_AIRPORT_CODE,
        MAX(COUNTRY_CODE) AS COUNTRY_CODE
    FROM dim_airport_staging
    WHERE IATA_AIRPORT_CODE IS NOT NULL
    GROUP BY IATA_AIRPORT_CODE
)
SELECT
    ROW_NUMBER() OVER (ORDER BY IATA_AIRPORT_CODE) AS airport_id,
    IATA_AIRPORT_CODE,
    COUNTRY_CODE
FROM unique_airports;
```

#### Dimenzia dim_date

Dimenzia dim_date uchováva dátumové informácie spolu s odvodenými atribútmi, ako sú deň, mesiac, rok a štvrťrok. Hoci aktuálny dataset obsahuje iba jeden dátum, dimenzia je pripravená na budúce rozšírenie o ďalšie dátumy bez nutnosti zmeny štruktúry.

Dimenzia je navrhnutá ako SCD Typ 0, pretože kalendárne atribúty sú nemenné. Nové záznamy sú do dimenzie iba dopĺňané v prípade výskytu nových dátumov v dátach.

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD')) AS date_id,
    date,
    DAY(date) AS day,
    MONTH(date) AS month,
    YEAR(date) AS year,
    QUARTER(date) AS quarter
FROM dim_date_staging;
```

#### Dimenzia dim_time

Dimenzia dim_time bola vytvorená ako kompletná časová dimenzia obsahujúca všetkých 1440 minút dňa. Pre každý časový bod obsahuje:

- `hodinu a minútu`
- `časť dňa (morning, afternoon, evening, night)`

Dimenzia je typu SCD Typ 0, keďže časové atribúty sú nemenné a univerzálne platné.

```sql
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
FROM minutes;
```

#### Dimenzia dim_flight_state

Dimenzia dim_flight_state obsahuje všetky unikátne stavy letu (napr. Scheduled, InAir, Landed). Oddelenie stavu letu do samostatnej dimenzie zabraňuje duplicite údajov a zachováva správny grain faktovej tabuľky.

```sql
CREATE OR REPLACE TABLE dim_flight_state AS
WITH unique_states AS (
    SELECT DISTINCT FLIGHT_STATE
    FROM dim_flight_state_staging
)
SELECT
    ROW_NUMBER() OVER (ORDER BY FLIGHT_STATE) AS flight_state_id,
    FLIGHT_STATE
FROM unique_states;
```

---

### Faktová tabuľka fact_flight_status

Faktová tabuľka fact_flight_status reprezentuje jeden stav letu v konkrétnom čase. Obsahuje:

- `cudzie kľúče na všetky dimenzie`

- `metriky meškania odletu a príletu`

- `kapacitné údaje (skutočný a predikovaný počet sedadiel)`

Meškania boli vypočítané ako rozdiel medzi plánovanými a skutočnými časmi odletu a príletu. Pri napĺňaní faktovej tabuľky boli použité LEFT JOIN, aby sa zachovali všetky záznamy zo staging tabuľky aj v prípadoch, keď neexistovala zodpovedajúca hodnota v niektorej dimenzii.

```sql
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
    dt_actual_depart.time_id AS actual_departure_time_id,
    dt_actual_arrive.time_id AS actual_arrival_time_id,
    f.SCHEDULED_DEPARTURE_TIME_LOCAL AS scheduled_departure_time,
    f.SCHEDULED_ARRIVAL_TIME_LOCAL AS scheduled_arrival_time,
    f.DEPARTURE_ACTUAL_OUTGATE_LOCAL AS actual_departure_time,
    f.ARRIVAL_ACTUAL_INGATE_LOCAL AS actual_arrival_time,
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
LEFT JOIN dim_time dt_arrive ON TO_TIME(f.SCHEDULED_ARRIVAL_TIME_LOCAL) = dt_arrive.time
LEFT JOIN dim_time dt_actual_depart ON TO_TIME(f.DEPARTURE_ACTUAL_OUTGATE_LOCAL) = dt_actual_depart.time
LEFT JOIN dim_time dt_actual_arrive ON TO_TIME(f.ARRIVAL_ACTUAL_INGATE_LOCAL) = dt_actual_arrive.time;

```

Výsledkom transform fázy je plne funkčný hviezdicový dátový model, pripravený na analytické dotazy a vizualizácie.

---

## 4. Vizualizácia dát

Dashboard obsahuje 7 vizualizácií, ktoré poskytujú prehľad o kľúčových metríkach a trendoch súvisiacich s výkonom letov, stavmi letov, meškaniami a kapacitami leteckých spoločností. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť prevádzku a dynamiku leteckej dopravy.

### Graf 1: Priemerné meškanie podľa leteckej spoločnosti

---Image here

Táto vizualizácia zobrazuje priemerné príletové meškanie pre jednotlivé letecké spoločnosti. Umožňuje identifikovať spoločnosti s najväčšími meškaniami, čo môže byť základom pre analýzu príčin a optimalizáciu prevádzky.

```sql
SELECT
    a.IATA_CARRIER_CODE,
    AVG(f.arrival_delay_minutes) AS avg_arrival_delay
FROM fact_flight_status f
JOIN dim_airline a ON f.airline_id = a.airline_id
WHERE f.arrival_delay_minutes > 0
GROUP BY a.IATA_CARRIER_CODE
ORDER BY avg_arrival_delay DESC;
```

### Graf 2: Počet letov podľa stavu letu

---Image here

Graf znázorňuje rozdelenie počtu letov podľa ich aktuálneho stavu (napríklad odlet, prílet, meškanie, zrušenie). Táto vizualizácia pomáha pochopiť, v akých fázach sa najviac letov nachádza.

```sql
SELECT
    fs.FLIGHT_STATE,
    COUNT(*) AS flight_count
FROM fact_flight_status f
JOIN dim_flight_state fs
    ON f.flight_state_id = fs.flight_state_id
GROUP BY fs.FLIGHT_STATE
ORDER BY flight_count DESC;
```

### Graf 3: Počet letov podľa časti dňa

---Image here

Tento graf ukazuje, koľko letov je plánovaných v jednotlivých častiach dňa (ráno, popoludnie, večer, noc). Pomáha identifikovať časové obdobia s najvyššou leteckou aktivitou.

```sql
SELECT
    t.part_of_day,
    COUNT(*) AS flight_count
FROM fact_flight_status f
JOIN dim_time t
  ON f.scheduled_departure_time_id = t.time_id
GROUP BY t.part_of_day
ORDER BY flight_count DESC;
```

### Graf 4: Porovnanie skutočných vs. predikovaných sedadiel podľa leteckej spoločnosti

---Image here

Graf porovnáva priemerný počet skutočne dostupných sedadiel oproti predikovaným hodnotám pre jednotlivé letecké spoločnosti. Pomáha sledovať presnosť predpovedí kapacity.

```sql
SELECT
    a.IATA_CARRIER_CODE,
    AVG(f.ACTUAL_TOTAL_SEATS) AS avg_actual_seats,
    AVG(f.PREDICTED_TOTAL_SEATS) AS avg_predicted_seats
FROM fact_flight_status f
JOIN dim_airline a ON f.airline_id = a.airline_id
GROUP BY a.IATA_CARRIER_CODE
ORDER BY avg_actual_seats DESC;
```

### Graf 5: Top 10 destinácií podľa počtu letov

---Image here

Táto vizualizácia zobrazuje desať krajín s najväčším počtom príletov. Umožňuje rýchlo identifikovať najfrekventovanejšie cieľové destinácie leteckej dopravy.

```sql
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
```

### Graf 6: Priemerné meškanie odletov počas dňa

---Image here

Graf znázorňuje priemerné odletové meškanie (v minútach) podľa hodiny dňa. Umožňuje odhaliť časové obdobia, kedy dochádza k najväčším meškaniam.

```sql
SELECT
    t.hour,
    AVG(f.departure_delay_minutes) AS avg_departure_delay
FROM fact_flight_status f
JOIN dim_time t ON f.scheduled_departure_time_id = t.time_id
GROUP BY t.hour
ORDER BY t.hour;
```

### Graf 7: Priemerné meškanie odletov podľa letiska a časti dňa (Top 25 letísk)

---Image here

Táto vizualizácia ukazuje priemerné meškanie odletov v rôznych častiach dňa pre top 25 letísk podľa počtu letov. Pomáha identifikovať časové a lokálne vzory v meškaní, čo je užitočné pre optimalizáciu prevádzky a plánovanie.

```sql
WITH top_airports AS (
    SELECT ap.airport_id, ap.IATA_AIRPORT_CODE, COUNT(*) AS flight_count
    FROM fact_flight_status f
    JOIN dim_airport ap ON f.departure_airport_id = ap.airport_id
    GROUP BY ap.airport_id, ap.IATA_AIRPORT_CODE
    ORDER BY flight_count DESC
    LIMIT 25
)
SELECT
    ta.IATA_AIRPORT_CODE,
    t.part_of_day,
    AVG(f.departure_delay_minutes) AS avg_delay
FROM fact_flight_status f
JOIN top_airports ta ON f.departure_airport_id = ta.airport_id
JOIN dim_time t ON f.scheduled_departure_time_id = t.time_id
GROUP BY ta.IATA_AIRPORT_CODE, t.part_of_day
ORDER BY ta.IATA_AIRPORT_CODE, t.part_of_day;
```

Dashboard poskytuje komplexný pohľad na prevádzku letov, umožňuje sledovať meškania, kapacity aj distribúciu letov podľa stavu, času a destinácie.

---

Marek Lörinc

