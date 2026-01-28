/*
=====================
Create Database and Schemas
=====================
Script Purpose: 
  This script creates a new database named 'DataWareHouse' after checking if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

USE master;

CREATE DATABASE DataWarehouse; 

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;


