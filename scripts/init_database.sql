use master;
CREATE DATABASE DataWarehouse;
use DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

"""
==============================================================================
Script Name : init_database.sql
Purpose     : 
    This script is designed to create a new SQL Server database named 
    'DataWarehouse' along with three schemas to represent the standard 
    data lake architecture layers: bronze, silver, and gold.

    - 'bronze' : For raw data ingestion.
    - 'silver' : For cleansed, transformed intermediate data.
    - 'gold'   : For final, business-ready data.

Script Usage:
    - Intended for use in SQL Server (MSSQL).
    - Requires execution in an environment that recognizes 'GO' 
      batch separators (e.g., SQL Server Management Studio (SSMS), Azure Data Studio).
    - Run the entire script as-is to create the database and its schemas.

Warnings:
    - Running this script will create a new database named 'DataWarehouse'.
      If a database with this name already exists, the script will fail unless 
      that database is first dropped manually.
    - This script does not check for existing databases or schemas. 
      Manual intervention may be required to avoid conflicts.
    - 'USE' and 'CREATE SCHEMA' must be executed in separate batches, 
      which is why 'GO' statements are included.
    - DO NOT remove or modify 'GO' statements unless you understand 
      SQL Server batch execution rules — improper modification will 
      result in errors such as:
        'CREATE SCHEMA must be the first statement in a query batch.'
    - The script does not create tables or objects inside these schemas — 
      it only creates the database and the empty schema containers.

Author      : Yash Gadhave
Created On  : 19-06-2025
License     : MIT License 
==============================================================================
"""
