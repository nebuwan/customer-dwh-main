/*
===============================================================================
Stored Procedure: bronze.usp_load_bronze
===============================================================================
Script Purpose:
    Loads raw data from external CSV files into the 'bronze' layer of the data warehouse.

    This procedure is responsible for:
    - Truncating existing data in staging (bronze) tables to ensure a clean load.
    - Using the `BULK INSERT` command to ingest data from predefined file paths.
    - Loading data from both CRM and ERP source systems into corresponding bronze tables.
    - Capturing and logging any errors encountered during the load into the dbo.error_log table.

Assumptions:
    - The source CSV files are located at the specified file paths accessible by SQL Server.
    - The file format uses a header row and comma as a field terminator.
    - The target tables already exist and match the structure of the CSV files.
    - A table named `dbo.error_log` exists to capture error details.

Parameters:
    None

Error Handling:
    - If an error occurs (e.g., missing file, schema mismatch), the procedure will:
        - Catch the error.
        - Insert the error details into the `dbo.error_log` table with metadata such as
          procedure name, error number, severity, state, line, and message.

Schema Affected:
    - [bronze] schema: target staging tables
    - [dbo] schema: used for the `error_log` table

Usage Example:
    EXEC bronze.usp_load_bronze;

Maintenance Notes:
    - If new source files/tables are added, corresponding TRUNCATE and BULK INSERT 
      statements must be appended.
    - File paths should be validated to ensure accessibility by the SQL Server process.
===============================================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.usp_load_bronze AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        DECLARE 
            @file_exists INT,
            @procedure SYSNAME = OBJECT_NAME(@@PROCID);

        -- Helper block for each file
        -- bronze.crm_cust_info
        EXEC master.dbo.xp_fileexist '/data/cust_info.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.crm_cust_info;
                BULK INSERT bronze.crm_cust_info
                FROM '/data/cust_info.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/cust_info.csv not found.', 16, 1);

        -- bronze.crm_prd_info
        EXEC master.dbo.xp_fileexist '/data/prd_info.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.crm_prd_info;
                BULK INSERT bronze.crm_prd_info
                FROM '/data/prd_info.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/prd_info.csv not found.', 16, 1);

        -- bronze.crm_sales_details
        EXEC master.dbo.xp_fileexist '/data/sales_details.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.crm_sales_details;
                BULK INSERT bronze.crm_sales_details
                FROM '/data/sales_details.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/sales_details.csv not found.', 16, 1);

        -- bronze.erp_loc_a101
        EXEC master.dbo.xp_fileexist '/data/loc_a101.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.erp_loc_a101;
                BULK INSERT bronze.erp_loc_a101
                FROM '/data/loc_a101.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/loc_a101.csv not found.', 16, 1);

        -- bronze.erp_cust_az12
        EXEC master.dbo.xp_fileexist '/data/cust_az12.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.erp_cust_az12;
                BULK INSERT bronze.erp_cust_az12
                FROM '/data/cust_az12.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/cust_az12.csv not found.', 16, 1);

        -- bronze.erp_px_cat_g1v2
        EXEC master.dbo.xp_fileexist '/data/px_cat_g1v2.csv', @file_exists OUTPUT;
        IF @file_exists = 1
            BEGIN
                TRUNCATE TABLE bronze.erp_px_cat_g1v2;
                BULK INSERT bronze.erp_px_cat_g1v2
                FROM '/data/px_cat_g1v2.csv'
                WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            END
        ELSE
            RAISERROR('File /data/px_cat_g1v2.csv not found.', 16, 1);

    END TRY

    BEGIN CATCH
        DECLARE 
            @ErrorNumber    INT = ERROR_NUMBER(),
            @ErrorSeverity  INT = ERROR_SEVERITY(),
            @ErrorState     INT = ERROR_STATE(),
            @ErrorProcedure SYSNAME = ERROR_PROCEDURE(),
            @ErrorLine      INT = ERROR_LINE(),
            @ErrorMessage   NVARCHAR(4000) = ERROR_MESSAGE();

        INSERT INTO dbo.error_log (
            procedure_name,
            error_number,
            error_severity,
            error_state,
            error_line,
            error_message
        )
        VALUES (
            @ErrorProcedure,
            @ErrorNumber,
            @ErrorSeverity,
            @ErrorState,
            @ErrorLine,
            @ErrorMessage
        );

    END CATCH
END
GO
