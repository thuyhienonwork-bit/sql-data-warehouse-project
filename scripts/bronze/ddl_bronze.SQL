/* 
==================================
DDL Script: Create Bronze Tables 
==================================
Script Purpose:
  This script creates tables in the 'bronze' schema', dropping existing tables if they already exist
  Run this script to re-define the DDL structure of 'bronze' Tables
===================================
*/

USE master;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT, 
    cst_key NVARCHAR (50),
    cst_firstname NVARCHAR (50),
    cst_lastname NVARCHAR (50),
    cst_material_status NVARCHAR (50),
    cst_gndr NVARCHAR (50),
    cst_create_date DATE 
);


IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR (50),
    prd_nm NVARCHAR (50),
    prd_cost INT, 
    prd_line NVARCHAR (50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR (50),
    sls_prd_key NVARCHAR (50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
); 




-- Insert data



        IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
            DROP TABLE bronze.erp_loc_a101;
        CREATE TABLE bronze.erp_loc_a101 (
            cid NVARCHAR (50),
            cntry NVARCHAR (50)
        );


        IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
            DROP TABLE bronze.erp_cust_az12;
        CREATE TABLE bronze.erp_cust_az12 (
            cid NVARCHAR (50),
            bdate DATE,
            gen NVARCHAR (50)
        );


        IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
            DROP TABLE bronze.erp_px_cat_g1v2;
        CREATE TABLE bronze.erp_px_cat_g1v2 (
            id NVARCHAR (50),
            cat NVARCHAR (50),
            subcat NVARCHAR (50),
            maintenance NVARCHAR (50)
        );

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
