-- CREATING A DATABASE NAMED 'DATAWAREHOUSEX' AND WE'LL TREAT IT AS A DATAWAREHOUSE
CREATE DATABASE DATAWAREHOUSEX;

-- IN A DATAWAREHOUSE, WE CAN HAVE DIFFERENT LAYERS. MAINLY STAGING AND CORE LAYERS
-- IN OUR PRACTICAL DATAWAREHOUSE, WE WILL CREATE 3 SCHEMAS AND TREAT THEM AS 3 DIFFERENT LAYERS
-- NAMES OF THE SCHEMAS CAN BE - 1. LANDING, 2. STAGING, 3. CORE
-- 1. LANDING SCHEMA -> IN OUR CASE, THIS SCHEMA WILL ACT AS A DATA SOURCE. WE WILL IMPORT FILES INTO IT.
-- 2. STAGING SCHEMA -> WE WILL EXTRACT DATA FROM DATASOURCE(PUBLIC SCHEMA) INTO THIS SCHEMA
-- 3. CORE SCHEMA -> HERE, WE WILL CREATE OUR FACTS AND DIMENSIONS TABLES.

CREATE SCHEMA IF NOT EXISTS LANDING;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;

-- IMPORTING DATA FILE TO LANDING SCHEMA (DATA SOURCE)
-- FOR THIS, CREATE A TABLE IN LANDING SCHEMA

CREATE TABLE landing.sales
(
    transaction_id integer,
    transactional_date timestamp,
    product_id character varying,
    customer_id integer,
    payment character varying,
    credit_card bigint,
    loyalty_card character varying,
    cost character varying,
    quantity integer,
    price numeric,
    PRIMARY KEY (transaction_id)
);

-- WE'VE NOT LOADED ANY DATA YET.
SELECT * FROM landing.sales;

-- LOAD DATA FILE TO THE TABLE USING COPY STATEMENT OR LOADING/IMPORT WIZARD

COPY landing.sales(transaction_id, transactional_date, product_id, customer_id, payment, credit_card, loyalty_card, cost, quantity, price)
FROM 'C:\Users\PUBLIC\Fact_Sales_1.csv'
DELIMITER ','
CSV HEADER;

-- VALIDATE THE LOADED DATA
SELECT * FROM landing.sales;

-- COUNT NO OF ROWS TO KNOW HOW MANY RECORDS ARE SERVED BY OUR DATA SOURCE.
SELECT COUNT(*) FROM landing.sales;

-- WE WILL GET THE DATA TO OUR STAGING LAYER FROM LANDING LAYER.
-- WE'LL JUST DO EXTRACTION, SO WE CAN HAVE SAME SCHEMA DEFINITION IN STAGING LAYER AS WELL.

-- STAGING LAYER
CREATE TABLE staging.sales
(
    transaction_id integer,
    transactional_date timestamp,
    product_id character varying,
    customer_id integer,
    payment character varying,
    credit_card bigint,
    loyalty_card character varying,
    cost character varying,
    quantity integer,
    price numeric,
    PRIMARY KEY (transaction_id)
);

-- WE'VE NOT LOADED ANY DATA YET. WE NEED TO EXTRACT FROM LANDING TO STAGE LAYER
SELECT * FROM staging.sales;

-- LETS LOAD DATA FROM FROM OUR DATA SOURCE TO STAGING AREA.
-- WE CAN DO THIS USING INSERT & SELECT STATEMENTS

INSERT INTO "staging".sales (
    transaction_id,
    transactional_date,
    product_id,
    customer_id,
    payment,
    credit_card,
    loyalty_card,
    cost,
    quantity,
    price
)
SELECT 
    transaction_id,
    transactional_date,
    product_id,
    customer_id,
    payment,
    credit_card,
    loyalty_card,
    cost, 
    quantity,
    price
FROM "landing".sales;

-- VALIDATE WHETHER THE DATA LOADED/NOT.
SELECT * FROM "staging".sales LIMIT 10;

-- 				 ***LETS WRITE THE CODE FOR INCREMENTAL LOAD AS WELL***

-- WE ARE GOING TO PERFORM THE INCREMENTAL LOAD BASED ON TRANSACTIONAL_DATE COL.
-- SO WE WILL JUST CREATE SOME DUMMY TABLE IN OUR STAGING SCHEMA TO KEEP TRACK OF THE DATA WE LOADED.
-- THIS TABLE WILL STORE A DUMMY TRANSACTIONAL_DATE AT THE START TO LOAD EVERYTHING FROM THE SOURCE
-- LATER, WE WILL UPDATE THIS TABLE AS WELL.

CREATE TABLE "staging".load_tracking (
    table_name varchar PRIMARY KEY,
    last_load_date timestamp
);

-- Initialize the load_tracking table for the sales table (SOME RANDOM VALUES)
INSERT INTO "staging".load_tracking (table_name, last_load_date)
VALUES ('sales', '1970-01-01 00:00:00');  -- Initial date far in the past

-- WE'LL USE MAX() FUNCTION TO GET THE LATEST TRANSACTIONAL DATE.
-- We'll also Use a Common Table Expression (CTE) to get the latest load date from the load_tracking table.

-- Get the latest load date for the sales table
WITH latest_load AS ( -- making use of CTE
    SELECT last_load_date
    FROM "staging".load_tracking
    WHERE table_name = 'sales'
)

-- Insert new records into the staging.sales table
INSERT INTO "staging".sales (
    transaction_id,
    transactional_date,
    product_id,
    customer_id,
    payment,
    credit_card,
    loyalty_card,
    cost,
    quantity,
    price
)
SELECT 
    transaction_id,
    transactional_date,
    product_id,
    customer_id,
    payment,
    credit_card,
    loyalty_card,
    cost,
    quantity,
    price
FROM "landing".sales l
WHERE l.transactional_date > (SELECT last_load_date FROM latest_load)
AND NOT EXISTS (
    SELECT 1
    FROM "staging".sales s
    WHERE s.transaction_id = l.transaction_id
);

-- Update the load_tracking table with the new max transactional_date
UPDATE "staging".load_tracking
SET last_load_date = (SELECT MAX(transactional_date) FROM "staging".sales)
WHERE table_name = 'sales';

-- VALIDATE THE STAGING DATA BEFORE MOVING TO CORE LAYER.
select * from staging.sales;


-- IN THE CORE LAYER, WE ARE GOING TO SET UP FACTS AND DIMENSIONS TABLES.

-- 						***** DIMENSION TABLES *****

-- from our data, we can create dimensions for : date, product, and payment. 
-- Below are the SQL commands for creating these dimension tables

-- 1. DATE DIMENION
CREATE TABLE core.dim_date (
    date_key bigint PRIMARY KEY,
    date_value date,
    year integer,
    month integer,
    day integer,
    weekday integer,
    quarter integer
);

-- Insert data into dim_date with conflict handling
INSERT INTO core.dim_date (date_key, date_value, year, month, day, weekday, quarter)
SELECT DISTINCT
    EXTRACT(year FROM transactional_date)*10000 + EXTRACT(month FROM transactional_date)*100 + EXTRACT(day FROM transactional_date) as date_key,
    transactional_date::date,
    EXTRACT(year FROM transactional_date) as year,
    EXTRACT(month FROM transactional_date) as month,
    EXTRACT(day FROM transactional_date) as day,
    EXTRACT(dow FROM transactional_date) as weekday,
    EXTRACT(quarter FROM transactional_date) as quarter
FROM staging.sales -- LOADING DATA FROM OUR STAGE LAYER
ON CONFLICT (date_key) DO NOTHING;

-- with 'conflict do nothing' -> we ensure that the insert operations for dimension tables handle duplicates 

-- VALIDATE THIS DIMENSION TABLE
SELECT * FROM CORE.DIM_DATE;

-- 2. PRODUCT DIMENSION
CREATE TABLE core.dim_product (
    product_pk integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    product_id character varying
);

-- Insert data into dim_product
INSERT INTO core.dim_product (product_id)
SELECT DISTINCT product_id
FROM staging.sales;

-- VALIDATE THE LOADED DATA
SELECT * FROM CORE.DIM_PRODUCT;

-- 3. PAYMENT DIMENSION (A JUNK DIMENSION)
CREATE TABLE core.dim_payment (
    payment_pk integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    payment character varying,
    loyalty_card character varying
);

-- Insert data into dim_payment
INSERT INTO core.dim_payment (payment, loyalty_card)
SELECT DISTINCT payment, loyalty_card
FROM staging.sales;

-- VALIDATE THE LOADED DATA
SELECT * FROM CORE.DIM_PAYMENT;

-- THIS PAYMENT_DIM TABLE IS CONTAINING SOME NULL VALUES. IT'D BETTER IF SOMEHOW WE CAN REPLACE THESE WITH OTHER VALUES
-- LETS FIRST TRUNCATE THE DATA FROM PAYMENT_DIM TABLE.
TRUNCATE TABLE CORE.DIM_PAYMENT;

-- VALIDATE THE DATA TRUNCATED/NOT
SELECT * FROM CORE.DIM_PAYMENT;

-- LOAD DATA AGAIN.
INSERT INTO core.dim_payment (payment, loyalty_card)
SELECT DISTINCT 
    COALESCE(payment, 'cash') as payment,
    loyalty_card
FROM staging.sales;

-- VALIDATE THE LOADED DATA
SELECT * FROM CORE.DIM_PAYMENT;


-- FROM THE DATA WE'VE, WE CAN CREATE OUR FACT TABLE THAT CONTAINS:
-- 1. DATEKEY FOR DATE DIMENSION
-- 2. PRODUCT FOREIGN KEY
-- 3. PAYMENT DIMENSION
-- 4. WE HAVE 3 MEASURES - COST, QUANTITY, PRICE. WE'LL CREATE SOME ADDITIONAL MEASURES.
-- THE CREDIT_CARD COLUMN CAN BEUSED AS A DEGENERATE DIMENSION SINCE IT WILL NOT BE ASSOCIATED TO ANY OTHER DATA.


--								***** FACT TABLE *****

CREATE TABLE core.sales (
    transaction_id integer PRIMARY KEY,
    transactional_date timestamp,
    transactional_date_fk bigint,
    product_id character varying,
    product_fk integer,
    customer_id integer,
    payment_fk integer,
    credit_card bigint,
    cost numeric,
    quantity integer,
    price numeric,
    total_cost numeric,
    total_price numeric,
    profit numeric
);


-- Populate the core.sales fact table with explicit type casting
INSERT INTO core.sales (
    transaction_id,
    transactional_date,
    transactional_date_fk,
    product_id,
    product_fk,
    customer_id,
    payment_fk,
    credit_card,
    cost,
    quantity,
    price,
    total_cost,
    total_price,
    profit
)
SELECT 
    f.transaction_id,
    f.transactional_date,
    EXTRACT(year FROM f.transactional_date)*10000 + EXTRACT(month FROM f.transactional_date)*100 + EXTRACT(day FROM f.transactional_date) as transactional_date_fk,
    f.product_id,
    p.product_pk as product_fk,
    f.customer_id,
    d.payment_pk as payment_fk,
    f.credit_card,
    f.cost::numeric,
    f.quantity,
    f.price::numeric,
    (f.cost::numeric * f.quantity) as total_cost,
    (f.price::numeric * f.quantity) as total_price,
    ((f.price::numeric - f.cost::numeric) * f.quantity) as profit
FROM staging.sales f
LEFT JOIN core.dim_payment d ON d.payment = COALESCE(f.payment, 'cash') AND d.loyalty_card = f.loyalty_card
LEFT JOIN core.dim_product p ON p.product_id = f.product_id
ORDER BY f.transaction_id;


-- VALIDATE THE LOADED DATA
SELECT * FROM CORE.SALES;

-- CHECK HOW MANY ROWS ARE LOADED
SELECT COUNT(*) FROM CORE.SALES;




























