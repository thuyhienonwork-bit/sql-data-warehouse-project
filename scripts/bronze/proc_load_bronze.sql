/*
===============================
Store Procedure: Load Bronze Layer (Source -> Bronze)
===============================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data
  - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables. 

Parameters:
  None
  This stored procedure does not accept any parameters or return any values. 

Usage Example:
  EXEC bronze.load_bronze;
===================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
        -- Insert cust_info data
        BULK INSERT bronze.crm_cust_info
        FROM '/data/source_crm/cust_info.csv'
        WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
        );

        SELECT COUNT(*) FROM bronze.crm_cust_info;


        -- Insert prd_info data 
        BULK INSERT bronze.crm_prd_info
        FROM '/data/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) FROM bronze.crm_prd_info;


        -- Insert data sales details
        BULK INSERT bronze.crm_sales_details
        FROM '/data/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT COUNT(*) FROM bronze.crm_sales_details


        -- Insert bronze.erp_cust
        BULK INSERT bronze.erp_cust_az12
        FROM '/data/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SELECT COUNT (*) FROM bronze.erp_cust_az12;


        -- Insert bronze.erp_loc_a101
        BULK INSERT bronze.erp_loc_a101
        FROM '/data/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SELECT COUNT (*) FROM bronze.erp_loc_a101;


        -- Insert bronze.erp_px_cat_g1v2
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/data/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',', 
            TABLOCK
        );

        SELECT COUNT (*) FROM bronze.erp_px_cat_g1v2; 
END; 
