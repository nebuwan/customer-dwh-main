    WITH bad_keys AS (
        SELECT cst_id
        FROM bronze.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL
    )
    SELECT 'CRM_CUST_INFO_PK_CHECK', 'Check for NULL or duplicate cst_id in silver.crm_cust_info',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'HIGH'
    FROM bad_keys;

WITH bad_trim AS (
        SELECT cst_key FROM bronze.crm_cust_info WHERE cst_key != TRIM(cst_key)
    )
    SELECT 'CRM_CUST_INFO_TRIM', 'Check for unwanted spaces in cst_key',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'LOW'
    FROM bad_trim;

 SELECT  cst_firstname
        ,TRIM(cst_firstname)
        ,CASE WHEN cst_firstname != TRIM(cst_firstname) THEN 'FAIL' ELSE 'PASS' END AS trim_check
        FROM bronze.crm_cust_info 
        WHERE 1=1
        --cst_firstname != TRIM(cst_firstname)
        ORDER BY trim_check asc