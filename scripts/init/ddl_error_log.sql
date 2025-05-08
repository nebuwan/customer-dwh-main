/*
===============================================================================
DDL Script: Create Error Log Table
===============================================================================
Script Purpose:
    This script creates the 'error_log' and 'quality_check_log' table in the 'dbo' schema for capturing
    errors encountered during ETL processes or stored procedure executions.
    
    The error log helps in tracking and troubleshooting issues by storing
    detailed error information including the procedure name, error number,
    severity, state, line, and message.

Usage Instructions:
    - Execute this script to create or re-create the error log table.
    - This table is typically used inside TRY...CATCH blocks to store runtime errors.
    - Ensure no critical logs are needed before dropping this table.

Notes:
    - The table is designed to grow over time; consider archiving or purging 
      older records periodically.
    - Can be extended to include timestamps, session/user context, or ETL job IDs.

===============================================================================
*/

USE CustomerCustomerDataWarehouse;
GO

IF OBJECT_ID('dbo.error_log', 'U') IS NOT NULL
    DROP TABLE dbo.error_log;
GO

CREATE TABLE dbo.error_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    procedure_name SYSNAME NOT NULL,
    error_number INT NOT NULL,
    error_severity INT NOT NULL,
    error_state INT NOT NULL,
    error_line INT NOT NULL,
    error_message NVARCHAR(4000) NOT NULL,
    log_timestamp DATETIME DEFAULT GETDATE()
);
GO

IF OBJECT_ID('dbo.quality_check_log', 'U') IS NOT NULL
    DROP TABLE dbo.quality_check_log; 
BEGIN
    CREATE TABLE dbo.quality_check_log (
        check_name VARCHAR(255),
        check_description VARCHAR(500),
        check_result VARCHAR(10),
        issue_count INT,
        severity VARCHAR(10),
        check_run_dt DATETIME DEFAULT GETDATE()
    );
END;
GO