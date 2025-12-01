-- Datetime dimension
DROP TABLE IF EXISTS taxi_dw.dim_datetime;
CREATE TABLE taxi_dw.dim_datetime (
    datetime_key  BIGINT PRIMARY KEY,
    pickup_date   DATE,
    pickup_year   INT,
    pickup_month  INT,
    pickup_day    INT,
    pickup_hour   INT,
    pickup_dow    INT
);

-- Vendor dimension
DROP TABLE IF EXISTS taxi_dw.dim_vendor;
CREATE TABLE taxi_dw.dim_vendor (
    vendor_key  BIGINT IDENTITY(1,1) PRIMARY KEY,
    vendorid    INT NOT NULL,
    vendor_name VARCHAR(50)
);

-- Rate code dimension
DROP TABLE IF EXISTS taxi_dw.dim_rate_code;
CREATE TABLE taxi_dw.dim_rate_code (
    ratecode_key  BIGINT IDENTITY(1,1) PRIMARY KEY,
    ratecodeid    INT NOT NULL,
    ratecode_desc VARCHAR(50)
);

-- Payment type dimension
DROP TABLE IF EXISTS taxi_dw.dim_payment_type;
CREATE TABLE taxi_dw.dim_payment_type (
    payment_type_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    payment_type     INT NOT NULL,
    payment_desc     VARCHAR(50)
);
