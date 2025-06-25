SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_material_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info]

SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_pred_info]

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]

SELECT TOP 1000 * FROM bronze.crm_cust_info;
SELECT TOP 1000 * FROM bronze.crm_pred_info;
SELECT TOP 1000 * FROM bronze.crm_sales_details;

-- so we can join sls_prd_key & sls_cust_id
-- also join all dates 3 cols 
-- crm_sales_details 
            -- > is an event & transactional table for sales
            -- > can  be use to connect with other tables

-- erp tables

SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
  FROM [DataWarehouse].[bronze].[erp_cust_az12]

SELECT TOP (1000) [cid]
      ,[cntry]
  FROM [DataWarehouse].[bronze].[erp_loc_a101]

SELECT TOP (1000) [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

SELECT TOP 1000 * FROM bronze.erp_cust_az12; -- cid , bday , gen
SELECT TOP 1000 * FROM bronze.erp_loc_a101; -- cid , cntry
SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2; -- id , cat , subcat , maintenance

print('crm_cust_info & erp_cust_az12 has common column CUST_ID so join them')
print('crm_pred_info & erp_px_cat_g1v2 has common values ID & PRD_KEY so join them')
