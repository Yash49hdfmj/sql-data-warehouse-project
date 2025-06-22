-- FULL LOAD = Truncate + insert
-- 1 st crm table
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
-- add try catch
-- ensures error handling, data integrity, issue logging 
-- for easier debugging
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		-- use DATEDIFF() 
		-- calculates diff between 2 dates
		-- return days,months,years
		SELECT * FROM bronze.crm_cust_info;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';
		
		-- 2nd crm table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_pred_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_pred_info
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SELECT * FROM bronze.crm_pred_info;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';

		-- 3rd crm table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details1.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT * FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';

		-- 1 st erp table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT * FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';

		-- 2nd erp table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101;';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT * FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';

		-- 3rd erp table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2;';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\91810\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SELECT * FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time)as NVARCHAR) + 'seconds';
		
		SET @batch_end_time = GETDATE();
		PRINT 'loading BRONZE LAYER is completed';
		PRINT 'total LOAD duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)as NVARCHAR) + 'seconds';
		
	END TRY
-- catch block will be executed only if SQL fails to LOAD TRY block
	BEGIN CATCH
	-- create a logging table & add msg inside
	PRINT '============================================'
	PRINT'error occured during bronze layer loading'
	PRINT 'error msg' + ERROR_MESSAGE();
	PRINT 'error msg' + CAST(ERROR_NUMBER()AS VARCHAR);
	PRINT 'error msg' + CAST(ERROR_STATE()AS VARCHAR);
	PRINT '============================================'

	END CATCH
END;

-- track ETL Duration 
-- i.e. time taken to load the tables in EXEC query
-- helps to understand BOTTLENECKS, OPTIMIZE PERFORMANCE, MONITOR TRENDS, DETECT ISSUES

EXEC bronze.load_bronze;
