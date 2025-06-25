CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
--------------------------------------------------------------------------------------------------------------
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
			SET @batch_start_time  = GETDATE();
			PRINT('======================================')
			PRINT('LOADING SILVER LAYER')
			PRINT('======================================')

			SET @start_time = GETDATE();
			PRINT('Truncate table bronze.crm_cust_info');
			TRUNCATE TABLE bronze.crm_cust_info
			PRINT('Inserting table bronze.crm_cust_info');
			-- cleanse crm_cust_info table 1
			PRINT'QUALITY CHECK A primary key must be unique & not NULL , not duplicate'
			SELECT 
			cst_id,
			COUNT(*) FROM bronze.crm_cust_info
			GROUP BY cst_id
			HAVING COUNT(*) >1 OR cst_id is NULL

			/* so this fields have duplicates
				cst_id (no col name)
				29449		2
				29473		2
				29433		2
				NULL		3
				29483		2
				29466		3
			*/
			PRINT ('<<Ranking Window Function cst_id = 29466')

			SELECT *,
			ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_test
			FROM bronze.crm_cust_info 
			WHERE cst_id = 29466 

			-- so basically 29466 repeated 3 times 
			-- based on cst_create_date ranking function highest one
			-- window function 
					--> row_number() : assign a unique row no. in result_set,
									-- based on defined order 
									-- will add new col as flag_test rank-wise
			PRINT('-------------------------------------------------------')
			select * from (
			SELECT *,
			ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_test
			FROM bronze.crm_cust_info )t WHERE flag_test = 1 

			-- quality check unwanted spaces in string vals
			SELECT cst_firstname FROM bronze.crm_cust_info
			WHERE cst_firstname != TRIM(cst_firstname)

			SELECT cst_lastname FROM bronze.crm_cust_info
			WHERE cst_lastname != TRIM(cst_lastname)
			-- trim() removes leading & trailing spaces from string

			PRINT('Cardinality consistency of MARRIED , GENDER columns')
			-- Quality check cardinality cols Married, Gender
			-- consistency of vals in it
			SELECT DISTINCT cst_gndr
			FROM bronze.crm_cust_info;
			print('Null , M , F')
			-------------------------------------------------------
			PRINT('--------------------------------------')
			PRINT('<< TRANSFORMATIONS to clean up columns')
			PRINT('--------------------------------------')


			INSERT INTO silver.crm_cust_info(
			cst_id ,
			cst_key ,
			cst_firstname ,
			cst_lastname ,
			cst_material_status ,
			cst_gndr ,
			cst_create_date 
			)
			SELECT
			cst_id ,
			cst_key ,
			TRIM(cst_firstname) as cst_firstname ,
			TRIM(cst_lastname) as cst_lastname,
			CASE WHEN cst_material_status ='S' THEN 'Single'-- note it's an TYPO marital status 
				 WHEN cst_material_status ='M' THEN 'Married'
				 ELSE 'Sadhu'
			END cst_material_status, 
			CASE WHEN cst_gndr = 'F' THEN 'Female'
				 WHEN cst_gndr = 'M' THEN 'Male' 
				 ELSE 'unknown'
			END cst_gndr,
			cst_create_date 
 
			FROM (
				SELECT *,
			ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_test
			FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)t WHERE flag_test =1;

			select * from silver.crm_cust_info; -- you can check same QUALITY check done before for BRONZE.CRM_CST_INFO
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

			--------------------------------------------------------------------------------------------------------------------
			PRINT('======================================')
			SET @start_time = GETDATE();
			PRINT('Truncate table silver.crm_pred_info');
			TRUNCATE TABLE silver.crm_pred_info
			PRINT('Inserting table silver.crm_pred_info');
			INSERT INTO silver.crm_pred_info(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
			)

			SELECT 
			prd_id ,
			REPLACE(SUBSTRING (prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm ,
			ISNULL(prd_cost,0) AS prd_cost ,
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Tourist'
				 ELSE 'Unknown'
			END prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt ,	-- cast due to time is in 00:00:00 not needed anymore
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
			FROM bronze.crm_pred_info


			/*
			WHERE SUBSTRING(prd_key,7,LEN(prd_key))IN (
			SELECT sls_prd_key FROM bronze.crm_sales_details)
			-- SUBSTRING extract a specific part of string value

			/*
			SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2;
			-- this table has same id values as crm_pred_info SUBSTRING cat_id
			PRINT ('crm_pred_info has cat_id as "CO-RF" ')
			PRINT ('erp_px_cat_g1v2 has id as "CO_RF" ')
			-- so replace that as '-' to '_'
			*/

			PRINT('Check unwanted spaces prd_nm')
			select prd_nm 
			from bronze.crm_pred_info
			where prd_nm != TRIM(prd_nm)

			PRINT('Check Nulls & negative numbers in prd_cost')
			select prd_cost
			from bronze.crm_pred_info
			where prd_cost<0 OR prd_cost IS NULL		 -- 2 vals are null here

			-- use ISNULL or COALESCE to replace with 0 

			PRINT('Standardization') 
			SELECT DISTINCT prd_line FROM bronze.crm_pred_info; -- Null, m , r , s , t

			-- check for invalid DATE orders
			SELECT * FROM bronze.crm_pred_info 
			WHERE prd_end_dt < prd_start_dt;

			*/

			SELECT * FROM silver.crm_pred_info;
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			---------------------------------------------------------------------------------------------------------------------------
			PRINT('======================================')
			SET @start_time = GETDATE();
			PRINT('Truncate table silver.crm_sales_details');
			TRUNCATE TABLE silver.crm_sales_details
			PRINT('Inserting table silver.crm_sales_details');
			INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price
			)
			SELECT 
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt ,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_due_dt ,
			CASE WHEN sls_sales IS NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity ,
			CASE WHEN sls_price<=0 OR sls_price IS NULL 
					THEN sls_sales/sls_quantity
				ELSE sls_price
			END sls_price
			FROM bronze.crm_sales_details
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			/*
			where sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
			print('we can connect 2 tables with same cst_id')
			*/
			/*
			where sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_pred_info)
			print('we can connect this 2 tables with same prd_key')
			*/

			/*
			where sls_ord_num != TRIM(sls_ord_num)
			print('unwanted spaces')
			*/
			----------------------------------------------------------------------------------
			/*
			PRINT('check invalid dates')
			select NULLIF(sls_order_dt,0) sls_order_dt
			from bronze.crm_sales_details
			where sls_order_dt <=0 OR len(sls_order_dt) !=8
				  OR sls_order_dt >20500101
				  OR sls_order_dt < 19000101
			-- 17 values are 0
			-- 2 values dates are not 8 numbers like yyyymmdd
			-- no outlier dates higher than 2050 and lower than 1900

			PRINT('NULLIF returns null if 2 given vals are equal ')
			*/
			-----------------------------------------------------------------------------------------
			-- business rules 
			PRINT('if SALES is negative,zero,null derive it using QUANTITY and PRICE')
			PRINT('if PRICE is zero,null derive it using SALES and QUANTITY')
			PRINT('if PRICE is NEGATIVE convert to POSITIVE')
			/*
			Sum(sales) = Quantity * Price
			No Negatives , Nulls , Zeros 
			*/
			---------------------------------------------------------------------------------
			/*
			SELECT 
			sls_sales AS old_sls_sales, 
			sls_quantity, 
			sls_price AS old_sls_price,

			CASE WHEN sls_sales IS NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,

			CASE WHEN sls_price<=0 OR sls_price IS NULL 
					THEN sls_sales/sls_quantity
				ELSE sls_price
			END sls_price

			FROM bronze.crm_sales_details	

			WHERE sls_sales != sls_quantity * sls_price
			OR sls_sales iS NULL OR sls_quantity IS NULL OR sls_price IS NULL
			OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
			ORDER BY sls_sales,sls_quantity,sls_price
			*/
			-----------------------------------------------------------------------------
			-----------------------------------------------------------------------------------------------------------------------------
			PRINT('======================================')
			SET @start_time = GETDATE();
			PRINT('Truncate table silver.erp_cust_az12');
			TRUNCATE TABLE silver.erp_cust_az12
			PRINT('Inserting table silver.erp_cust_az12');
			INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
			SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
				 ELSE 'Unknown'
			END AS gen
			FROM bronze.erp_cust_az12

			/*


			select * from [silver].[crm_cust_info]

			-- outliers in bdate
			SELECT DISTINCT
			bdate
			FROM bronze.erp_cust_az12 
			WHERE bdate <'1916-01-01' OR bdate > GETDATE()

			-- standardization gen column
			select distinct gen from bronze.erp_cust_az12 -- Null, F, , male,female,M

			*/

			select * from silver.erp_cust_az12;
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

			--------------------------------------------------------------------------------------------------------------------------
			PRINT('======================================')
			SET @start_time = GETDATE();
			PRINT('Truncate table silver.erp_loc_a101');
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT('Inserting table silver.erp_loc_a101');
			INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
			)
			select
			REPLACE(cid,'-','')cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
				 WHEN TRIM(cntry)  IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
				 ELSE TRIM(cntry)
			END AS cntry
			from bronze.erp_loc_a101;
			select * from [silver].[crm_cust_info];
			select * from silver.erp_loc_a101;
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

			-- both tables have same cid & cst_key just difference is '-'
			-- REPLACE ()
			/*
			OR
			from bronze.erp_loc_a101 WHERE REPLACE(cid,'-','') NOT IN
			select cst_key from silver.crm_cust_info;
			*/

			----------------------------------------------------------------------
			/*

			PRINT('low cardinality in cntry column perform Standardization')
			SELECT DISTINCT cntry FROM bronze.erp_loc_a101 
			ORDER BY cntry; 
			-- null, empty , aus,US, USA , United States , DE.....

			*/
			-------------------------------------------------------------------------------------------------------------------------------
			PRINT('======================================')
			SET @start_time = GETDATE();
			PRINT('Truncate table silver.erp_px_cat_g1v2');
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT('Inserting table silver.erp_px_cat_g1v2');
			INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
			)
			SELECT 
			id,
			cat,
			subcat,
			maintenance
			FROM bronze.erp_px_cat_g1v2;

			select * from [silver].[crm_pred_info];
			-- id & cat_id are same in both columns 

			PRINT('check unwanted spaces')
			/*
			SELECT cat FROM bronze.erp_px_cat_g1v2
			WHERE cat != TRIM(cat) 
			OR subcat != TRIM(subcat)
			OR maintenance != TRIM(maintenance);
			*/
			SET @end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

			SET @batch_end_time = GETDATE();
			PRINT '>> load duration is :' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		END TRY
		BEGIN CATCH 
			PRINT '--------------------------------------'
			PRINT 'error msg' + ERROR_MESSAGE();
			PRINT 'error msg' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'error msg' + CAST(ERROR_STATE() AS NVARCHAR);
		END CATCH
END
EXEC silver.load_silver;
