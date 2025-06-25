CREATE VIEW gold.dim_products AS

SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt , pn.prd_key) AS product_key, -- surrogate primary key
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance AS maintainance,
pn.prd_cost AS product_cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS product_start
FROM silver.crm_pred_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id

WHERE prd_end_dt IS NULL		-- FILTER OUT all historical data
-- if prd_end_date is null then it is current info of the product

SELECT * FROM gold.dim_products;