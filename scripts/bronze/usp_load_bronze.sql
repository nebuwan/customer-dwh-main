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

USE CustomerDataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.usp_load_bronze AS
BEGIN
    SET NOCOUNT ON;  -- Prevents extra result sets from interfering with logic
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;  -- Avoids locking when reading system views, if any

    --------------------------------------------------------------------------------
    -- Declare variables for control flow, error handling, and load logging
    --------------------------------------------------------------------------------
    DECLARE 
        @file_exists      INT,                           -- Output from xp_fileexist to check if file exists
        @procedure        SYSNAME = OBJECT_NAME(@@PROCID), -- Current procedure name for logging
        @load_start       DATETIME = GETDATE(),          -- Capture start time of the procedure
        @load_end         DATETIME,                      -- Will store the load end time
        @duration         INT,                           -- Duration of load (in seconds)
        @status           NVARCHAR(20) = 'SUCCESS',      -- Initial load status (assumed success)
        @error_message    NVARCHAR(4000) = NULL;         -- Will store error message in case of failure

    BEGIN TRY
        --------------------------------------------------------------------------------
        -- File Check & Load Block: CRM Customer Info
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/cust_info.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.crm_cust_info;
            BULK INSERT bronze.crm_cust_info
            FROM '/data/cust_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);  -- Skips header row
        END
        ELSE RAISERROR('File /data/cust_info.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- File Check & Load Block: CRM Product Info
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/prd_info.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.crm_prd_info;
            BULK INSERT bronze.crm_prd_info
            FROM '/data/prd_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        END
        ELSE RAISERROR('File /data/prd_info.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- File Check & Load Block: CRM Sales Details
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/sales_details.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.crm_sales_details;
            BULK INSERT bronze.crm_sales_details
            FROM '/data/sales_details.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        END
        ELSE RAISERROR('File /data/sales_details.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- File Check & Load Block: ERP Location Info
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/loc_a101.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.erp_loc_a101;
            BULK INSERT bronze.erp_loc_a101
            FROM '/data/loc_a101.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        END
        ELSE RAISERROR('File /data/loc_a101.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- File Check & Load Block: ERP Customer Demographics
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/cust_az12.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.erp_cust_az12;
            BULK INSERT bronze.erp_cust_az12
            FROM '/data/cust_az12.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        END
        ELSE RAISERROR('File /data/cust_az12.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- File Check & Load Block: ERP Product Category
        --------------------------------------------------------------------------------
        EXEC master.dbo.xp_fileexist '/data/px_cat_g1v2.csv', @file_exists OUTPUT;
        IF @file_exists = 1
        BEGIN
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM '/data/px_cat_g1v2.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        END
        ELSE RAISERROR('File /data/px_cat_g1v2.csv not found.', 16, 1);

        --------------------------------------------------------------------------------
        -- Load Logging: Log Success
        --------------------------------------------------------------------------------
        SET @load_end = GETDATE();
        SET @duration = DATEDIFF(SECOND, @load_start, @load_end);

        INSERT INTO dbo.load_log (
            procedure_name, load_start_time, load_end_time, duration_seconds, status, error_message
        )
        VALUES (
            @procedure, @load_start, @load_end, @duration, @status, @error_message
        );
    END TRY

    BEGIN CATCH
        --------------------------------------------------------------------------------
        -- Error Logging: Capture and Log Detailed Error Info
        --------------------------------------------------------------------------------
        DECLARE 
            @ErrorNumber    INT = ERROR_NUMBER(),
            @ErrorSeverity  INT = ERROR_SEVERITY(),
            @ErrorState     INT = ERROR_STATE(),
            @ErrorProcedure SYSNAME = ERROR_PROCEDURE(),
            @ErrorLine      INT = ERROR_LINE(),
            @ErrorMessage   NVARCHAR(4000) = ERROR_MESSAGE();

        -- Log technical metadata to dbo.error_log
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

        -- Also log summary failure info to dbo.load_log
        SET @status = 'FAILED';
        SET @load_end = GETDATE();
        SET @duration = DATEDIFF(SECOND, @load_start, @load_end);
        SET @error_message = @ErrorMessage;

        INSERT INTO dbo.load_log (
            procedure_name, load_start_time, load_end_time, duration_seconds, status, error_message
        )
        VALUES (
            @ErrorProcedure, @load_start, @load_end, @duration, @status, @error_message
        );
    END CATCH
END
GO
