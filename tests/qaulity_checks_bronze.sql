-- ===========================================
-- Quality Checks on bronze.crm_cust_info table
-- ===========================================

-- 1. Identify duplicate or null customer IDs
--    - Ensures that the 'cst_id' column contains unique and non-null values.
SELECT cst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- 2. Detect customer keys with leading or trailing spaces
--    - Trimming helps maintain consistency, especially for joins or lookups.
SELECT cst_key 
FROM bronze.crm_cust_info 
WHERE cst_key != TRIM(cst_key);


-- 3. Check for first and last names with leading or trailing spaces
--    - Ensures clean and standardized names for reporting or downstream usage.
SELECT cst_firstname, cst_lastname
FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname) 
   OR cst_lastname != TRIM(cst_lastname);


-- 4. Review distinct values in the gender column
--    - Helps identify typos or inconsistent coding (e.g., 'M', 'Male', 'm').
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;



