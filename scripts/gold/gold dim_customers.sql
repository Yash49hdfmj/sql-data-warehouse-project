PRINT('Building GOLD LAYER')

/*

SELECT cst_id, COUNT(*) FROM	-- SUBQUERY check duplicates by JOINS 
(
SELECT 
ci.cst_id, 
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_gndr,
ci.cst_material_status,
ci.cst_create_date,
ci.dwh_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
)t GROUP BY cst_id	-- cid is primary key here
HAVING COUNT(*) >1

-- ci is alias we're going to use later on
PRINT('Inner join can cause lose of data because we dont know it contains all customer info or not')
PRINT('LEFT JOIN - MASTER to other tables same columns')
PRINT('CHECK IF ANY DUPLICATES CAUSED BY JOINS ')

*/
CREATE VIEW gold.dim_customers AS 

SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,		-- window() surrogate_key
ci.cst_id as customer_id, 
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the MASTER for gender 
	ELSE COALESCE(ca.gen,'Unknown')
END as gender,
ci.cst_material_status as marital_status,
ca.bdate as birth_date,
ci.cst_create_date as create_date,
ci.dwh_create_date as silver_load_time


FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

select * from gold.dim_customers;
select distinct gender from gold.dim_customers;
-- now we've 2 sources GENDER from CRM and ERP
-- change column names 
-- sort columns accordingly priority

PRINT('DIMENSTION TABLES')
PRINT('first_name, last_name, country, gender, marital, birth_date, create_date, silver_load_time')

PRINT('PRIMARY KEY cid')
-- surrogate key 
-- are system generated keys unique identifier assign to each record in a table
-- it has no business meaning and no one in business knows about it
-- only to control connecting data model

-- 2 ways to create surrogate keys
-- DDL based generation
-- Query based using WINDOW( row_number ) 
