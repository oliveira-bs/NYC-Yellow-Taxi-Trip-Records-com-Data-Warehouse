-- Criar o esquema do banco (opcional)
CREATE SCHEMA nyc_taxi_dw;
SET search_path TO nyc_taxi_dw;

-- 1. Dimensão de Tempo
DROP TABLE IF EXISTS time_dim CASCADE;
CREATE TABLE IF NOT EXISTS time_dim (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    hour INT,
    minute INT,
    second INT,
    part_of_day VARCHAR(20) -- ex: Morning, Afternoon, Evening
);

-- 2. Dimensão de Fornecedor
DROP TABLE IF EXISTS vendor_dim CASCADE;
CREATE TABLE IF NOT EXISTS vendor_dim (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(255)
);

-- 3. Dimensão de Localização
DROP TABLE IF EXISTS location_dim CASCADE;
CREATE TABLE IF NOT EXISTS location_dim (
    location_id INT PRIMARY KEY,
    borough VARCHAR(100),
    zone VARCHAR(255),
    service_zone VARCHAR(100)
);

-- 4. Dimensão de Código de Tarifa
DROP TABLE IF EXISTS rate_code_dim CASCADE;
CREATE TABLE IF NOT EXISTS rate_code_dim (
    rate_code_id INT PRIMARY KEY,
    rate_code_description VARCHAR(255)
);

-- 5. Dimensão de Tipo de Pagamento
DROP TABLE IF EXISTS payment_type_dim CASCADE;
CREATE TABLE IF NOT EXISTS payment_type_dim (
    payment_type_id INT PRIMARY KEY,
    payment_description VARCHAR(255)
);

-- 6. Tabela Fato de Viagens
DROP TABLE IF EXISTS trips_fact CASCADE;
CREATE TABLE IF NOT EXISTS trips_fact (
    trip_id INT PRIMARY KEY,
    vendor_id INT,
    pickup_time_id INT,
    dropoff_time_id INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    rate_code_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    payment_type_id INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    total_amount FLOAT,
    calc_trip_duration_seconds INT, -- Calculado como diferença entre pickup e dropoff
    load_datetime TIMESTAMP,
    CONSTRAINT fk_vendor FOREIGN KEY (vendor_id) REFERENCES vendor_dim (vendor_id),
    CONSTRAINT fk_pickup_time FOREIGN KEY (pickup_time_id) REFERENCES time_dim (time_id),
    CONSTRAINT fk_dropoff_time FOREIGN KEY (dropoff_time_id) REFERENCES time_dim (time_id),
    CONSTRAINT fk_pickup_location FOREIGN KEY (pickup_location_id) REFERENCES location_dim (location_id),
    CONSTRAINT fk_dropoff_location FOREIGN KEY (dropoff_location_id) REFERENCES location_dim (location_id),
    CONSTRAINT fk_rate_code FOREIGN KEY (rate_code_id) REFERENCES rate_code_dim (rate_code_id),
    CONSTRAINT fk_payment_type FOREIGN KEY (payment_type_id) REFERENCES payment_type_dim (payment_type_id)
);
