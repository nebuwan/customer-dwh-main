-- ====================================================================
-- Stored Procedure: usp_run_silver_layer_quality_checks
-- Purpose: Perform data quality checks on Silver Layer and log results
-- ====================================================================

USE CustomerDataWarehouse;
GO

CREATE OR ALTER PROCEDURE dbo.usp_run_silver_layer_quality_checks
AS
BEGIN
    SET NOCOUNT ON;
    -------------------------------
    -- CRM_CUST_INFO Primary Key Check
    -------------------------------
    WITH bad_keys AS (
        SELECT cst_id
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_CUST_INFO_PK_CHECK', 'Check for NULL or duplicate cst_id in silver.crm_cust_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'HIGH'
    FROM bad_keys;

    -------------------------------
    -- CRM_CUST_INFO Trim Check
    -------------------------------
    WITH bad_trim AS (
        SELECT cst_key FROM silver.crm_cust_info WHERE cst_key != TRIM(cst_key)
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_CUST_INFO_TRIM', 'Check for cst_key unwanted spaces in silver.crm_cust_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'LOW'
    FROM bad_trim;

    -------------------------------
    -- CRM_CUST_INFO Marital Status Values
    -------------------------------
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_CUST_INFO_MARITAL_STATUS', 'List of distinct cst_marital_status for review in silver.crm_cust_info', 'INFO', COUNT(DISTINCT cst_marital_status), 'LOW'
    FROM silver.crm_cust_info;

    -------------------------------
    -- CRM_PRD_INFO Primary Key Check
    -------------------------------
    WITH bad_prd_keys AS (
        SELECT prd_id FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 OR prd_id IS NULL
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_PRD_INFO_PK_CHECK', 'Check for NULL or duplicate prd_id in silver.crm_prd_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'HIGH'
    FROM bad_prd_keys;

    -------------------------------
    -- CRM_PRD_INFO Trim Check
    -------------------------------
    WITH bad_prd_trim AS (
        SELECT prd_nm FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_PRD_INFO_TRIM', 'Check for prd_nm unwanted spaces in silver.crm_prd_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'LOW'
    FROM bad_prd_trim;

    -------------------------------
    -- CRM_PRD_INFO Cost Validation
    -------------------------------
    WITH bad_costs AS (
        SELECT prd_cost FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_PRD_INFO_COST', 'Check for NULL or negative prd_cost in silver.crm_prd_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'HIGH'
    FROM bad_costs;

    -------------------------------
    -- CRM_PRD_INFO Line Check
    -------------------------------
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_PRD_INFO_LINE', 'List of distinct prd_line for review in silver.crm_prd_info', 'INFO', COUNT(DISTINCT prd_line), 'LOW'
    FROM silver.crm_prd_info;

    -------------------------------
    -- CRM_PRD_INFO Invalid Date Order
    -------------------------------
    WITH bad_prd_dates AS (
        SELECT * FROM silver.crm_prd_info WHERE prd_end_dt < prd_start_dt
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_PRD_INFO_DATE_ORDER', 'Check for prd_end_dt earlier than prd_start_dt in silver.crm_prd_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'MEDIUM'
    FROM bad_prd_dates;

    -------------------------------
    -- CRM_SALES_DETAILS Due Date Format and Range
    -------------------------------
    WITH bad_due_dt AS (
        SELECT sls_due_dt FROM bronze.crm_sales_details
        WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_SALES_DETAILS_DUE_DT_FORMAT', 'Check for invalid due date format/range in bronze.crm_sales_details',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'HIGH'
    FROM bad_due_dt;

    -------------------------------
    -- CRM_SALES_DETAILS Invalid Date Order
    -------------------------------
    WITH bad_sales_dt AS (
        SELECT * FROM silver.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_SALES_DETAILS_DATE_ORDER', 'Check for order date after shipping/due date in silver.crm_sales_details',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'MEDIUM'
    FROM bad_sales_dt;

    -------------------------------
    -- CRM_SALES_DETAILS Sales Consistency
    -------------------------------
    WITH bad_sales AS (
        SELECT * FROM silver.crm_sales_details
        WHERE sls_sales != sls_quantity * sls_price
           OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
           OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'CRM_SALES_DETAILS_CONSISTENCY', 'Check that sales = quantity * price and values are positive/non-null in silver.crm_sales_details',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'HIGH'
    FROM bad_sales;

    -------------------------------
    -- ERP_CUST_AZ12 Birthdate Check
    -------------------------------
    WITH bad_bdates AS (
        SELECT bdate FROM silver.erp_cust_az12 WHERE bdate < '1924-01-01' OR bdate > GETDATE()
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'ERP_CUST_AZ12_BIRTHDATE', 'Check for invalid birthdate range (1924-01-01 to Today) in silver.erp_cust_az12',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'MEDIUM'
    FROM bad_bdates;

    -------------------------------
    -- ERP_CUST_AZ12 Gender Standardization
    -------------------------------
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'ERP_CUST_AZ12_GEN', 'List of distinct gen values for review in silver.erp_cust_az12', 'INFO', COUNT(DISTINCT gen), 'LOW'
    FROM silver.erp_cust_az12;

    -------------------------------
    -- ERP_LOC_A101 Country Code Review
    -------------------------------
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'ERP_LOC_A101_CNTRY', 'List of distinct cntry values for review in silver.erp_loc_a101', 'INFO', COUNT(DISTINCT cntry), 'LOW'
    FROM silver.erp_loc_a101;

    -------------------------------
    -- ERP_PX_CAT_G1V2 Trim Check
    -------------------------------
    WITH bad_cat_trim AS (
        SELECT * FROM silver.erp_px_cat_g1v2
        WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'ERP_PX_CAT_TRIM', 'Check for unwanted spaces in cat/subcat/maintenance in silver.erp_px_cat_g1v2',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, COUNT(*), 'LOW'
    FROM bad_cat_trim;

    -------------------------------
    -- ERP_PX_CAT_G1V2 Maintenance Field Review
    -------------------------------
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'ERP_PX_CAT_MAINTENANCE', 'List of distinct maintenance values for review in silver.erp_px_cat_g1v2', 'INFO', COUNT(DISTINCT maintenance), 'LOW'
    FROM silver.erp_px_cat_g1v2;
END;
GO
