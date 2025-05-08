/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'CustomerDataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'CustomerDataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'CustomerDataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CustomerDataWarehouse')
BEGIN
    ALTER DATABASE CustomerDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CustomerDataWarehouse;
END;
GO

-- Create the 'CustomerDataWarehouse' database
CREATE DATABASE CustomerDataWarehouse;
GO

USE CustomerDataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
