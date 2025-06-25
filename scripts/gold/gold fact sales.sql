create view gold.fact_sales as 
SELECT 
sd.sls_ord_num AS order_number,
cu.customer_key,
pr.product_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_price AS price,
sd.sls_quantity AS quantity,
sd.sls_sales AS sales
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key  = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

GO

PRINT( ' sls_ord_num , sls_cust_id , sls_prd_key are KEYS ');
PRINT( ' sls_ship_dt , sls_order_dt , sls_due_dt are DATES ');
PRINT( ' sls_price , sls_quantity , sls_sales are MEASURES ');

-- we going to use SURROGATE KEYS from dimension tables gold.dim_customer & gold.dim_products
-- instead of ID use primary keys

PRINT('Foreign Key Integrity (Dimensions)')
select * from gold.fact_sales;

PRINT('Check if DIMENSION TABLES are joined perfectly to FACT TABLE')
SELECT * FROM gold.fact_sales as fc
LEFT JOIN gold.dim_customers as gdc
ON gdc.customer_key = fc.customer_key
LEFT JOIN gold.dim_products as gdp
ON gdp.product_key = fc.product_key
WHERE gdp.product_key IS NULL



-- its showing empty tables means FACT DIMENSIONS works perfectly together