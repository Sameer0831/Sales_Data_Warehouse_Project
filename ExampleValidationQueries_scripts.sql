-- Queries to Validate Data in the Core Layer

-- Let's validate the data in our core layer by running some SQL queries against the facts and dimensions tables.
-- These queries will help ensure that our data transformations and load processes have been executed correctly.

-- 1. Query to Validate the Count of Transactions
-- This query checks the total number of transactions in the core.sales table to ensure that all transactions have been loaded correctly.


SELECT COUNT(*) AS total_transactions
FROM core.sales;

-- Expected Outcome:
-- The output should show the total count of transactions loaded into the core.sales table.

-- 2. Query to Validate Data in Dimension Tables
-- This query checks for the existence of records in the `core.dim_product` and `core.dim_payment` tables.

SELECT 
    (SELECT COUNT(*) FROM core.dim_product) AS total_products,
    (SELECT COUNT(*) FROM core.dim_payment) AS total_payments;

-- Expected Outcome: 
-- The output should show the count of products and payments in their respective dimension tables.

-- 3. Query to Validate Foreign Key Relationships
-- This query ensures that every foreign key in the `core.sales` table matches an existing primary key in the `core.dim_product` and `core.dim_payment` tables.

SELECT 
    COUNT(*) AS unmatched_products
FROM core.sales s
LEFT JOIN core.dim_product p ON s.product_fk = p.product_pk
WHERE p.product_pk IS NULL;

SELECT 
    COUNT(*) AS unmatched_payments
FROM core.sales s
LEFT JOIN core.dim_payment d ON s.payment_fk = d.payment_pk
WHERE d.payment_pk IS NULL;


-- Expected Outcome:
-- Both queries should return 0, indicating that all foreign keys in `core.sales` are valid and exist in their respective dimension tables.

-- 4. Query to Calculate Total Sales and Profit
-- This query calculates the total sales and profit to ensure that the calculations for `total_cost`, `total_price`, and `profit` columns are correct.

SELECT 
    SUM(total_cost) AS total_cost,
    SUM(total_price) AS total_sales,
    SUM(profit) AS total_profit
FROM core.sales;

-- Expected Outcome:
-- The output should show the summed values for `total_cost`, `total_sales`, and `total_profit` columns.

-- 5. Query to Validate Date Dimension
-- This query ensures that all date keys in the `core.sales` table have corresponding entries in the `core.dim_date` table.

SELECT 
    COUNT(*) AS unmatched_dates
FROM core.sales s
LEFT JOIN core.dim_date d ON s.transactional_date_fk = d.date_key
WHERE d.date_key IS NULL;

-- Expected Outcome:**
-- The query should return 0, indicating that all date keys in the `core.sales` table exist in the `core.dim_date` table.

-- 6. Query to Validate Data Integrity and Uniqueness
-- This query checks for any duplicate transaction IDs in the `core.sales` table to ensure data uniqueness.

SELECT 
    transaction_id, 
    COUNT(*) AS cnt
FROM core.sales
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Expected Outcome:
-- The query should return no rows, indicating that there are no duplicate transaction IDs in the `core.sales` table.
-- Executing these queries validates the data in your core layer. Ensure that the results match the expected outcomes to confirm the correctness of your data warehouse project.

