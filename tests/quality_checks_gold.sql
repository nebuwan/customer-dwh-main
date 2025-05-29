/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
    - Stored Procedure: usp_run_gold_layer_quality_checks
    - Purpose: Perform data quality checks on Gold Layer and log results
-- ====================================================================
USE CustomerDataWarehouse;
GO

CREATE OR ALTER PROCEDURE dbo.usp_run_gold_layer_quality_checks
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------
    -- Uniqueness of Customer Key
    -------------------------------
    WITH duplicate_customer_keys AS (
        SELECT customer_key
        FROM gold.dim_customers
        GROUP BY customer_key
        HAVING COUNT(*) > 1
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'GOLD_DIM_CUSTOMERS_PK', 'Duplicate customer_key in gold.dim_customers',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'HIGH'
    FROM duplicate_customer_keys;

    -------------------------------
    -- Uniqueness of Product Key
    -------------------------------
    WITH duplicate_product_keys AS (
        SELECT product_key
        FROM gold.dim_products
        GROUP BY product_key
        HAVING COUNT(*) > 1
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'GOLD_DIM_PRODUCTS_PK', 'Duplicate product_key in gold.dim_products',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'HIGH'
    FROM duplicate_product_keys;

    -------------------------------
    -- Fact to Dimension Referential Integrity Check
    -------------------------------
    WITH broken_references AS (
        SELECT f.*
        FROM gold.fact_sales f
        LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
        WHERE c.customer_key IS NULL OR p.product_key IS NULL
    )
    INSERT INTO dbo.quality_check_log (check_name, check_description, check_result, issue_count, severity)
    SELECT 'GOLD_FACT_SALES_REF_INT', 'Referential integrity check between fact_sales and dimension tables',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
           COUNT(*), 'HIGH'
    FROM broken_references;

END;
GO
