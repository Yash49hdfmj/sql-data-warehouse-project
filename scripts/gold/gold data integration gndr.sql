  SELECT DISTINCT
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the MASTER for gender 
	ELSE COALESCE(ca.gen,'Unknown')
END AS new_gen
-- so this is DATA INTEGRATION 
-- 2 diff source_systems into 1

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT jOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
ORDER BY 1,2
-- error caused multi-part identifier due to forget to add alias ci. in FROM clause
PRINT('so here CRM gndr & ERP gen table have different genders
		So we will take CRM gender as accurate due to its a MASTER TABLE')
