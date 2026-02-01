CREATE PROCEDURE silver.load_silver
AS
BEGIN
    /* =========================
       Data Checking
    ========================= */

    SELECT
        cst_id,
        COUNT(*) AS cnt
    FROM silver.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;


    /* =========================
       Silver.crm_cust_info
    ========================= */

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_material_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;


    /* =========================
       Silver.crm_prd_info
    ========================= */

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key))         AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0)                         AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - 1
            AS DATE
        ) AS prd_key_dt
    FROM bronze.crm_prd_info;


    /* =========================
       Silver.crm_sales_details
    ========================= */

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_ord_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;


    /* =========================
       Check for Invalid Dates
    ========================= */

    SELECT
        NULLIF(sls_order_dt, 0) AS sls_order_dt
    FROM bronze.crm_sales_details
    WHERE LEN(sls_order_dt) < 8;


    /* =========================
       Check for Invalid Date Orders
    ========================= */

    SELECT
        *
    FROM bronze.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt
       OR sls_order_dt > sls_due_dt;


    /* =========================
       Check Data Consistency: Sales, Quantity, Price
       - Sales = Quantity * Price
       - Values must not be NULL, zero, or negative.
    ========================= */

    SELECT
        sls_sales,
        sls_quantity,
        sls_price
    FROM bronze.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL
       OR sls_quantity IS NULL
       OR sls_price IS NULL
       OR sls_sales <= 0
       OR sls_quantity <= 0
       OR sls_price <= 0
    ORDER BY sls_sales, sls_quantity, sls_price;


    /* Takaway:
       Through this process, I find that despite that sls_quantity obsevered no abnomaly,
       the sls_sales, and the sls_price has NULL, and negative values, hence I will fixed
       the data by filling wrong data through applying the calculation and available data
    */

    SELECT
        sls_sales AS old_sls_sales,
        sls_quantity,
        sls_price AS old_sls_price,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL
       OR sls_quantity IS NULL
       OR sls_price IS NULL
       OR sls_sales <= 0
       OR sls_quantity <= 0
       OR sls_price <= 0
    ORDER BY sls_sales, sls_quantity, sls_price;


    SELECT
        *
    FROM bronze.crm_sales_details;


    /* =========================
       Silver.erp_cust_az12
    ========================= */

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(REPLACE(gen, CHAR(13), '')) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(REPLACE(gen, CHAR(13), '')) IN ('M', 'MALE')   THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12
    WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);


    /* =========================
       Identify Out-of-Range Dates
    ========================= */

    SELECT DISTINCT
        bdate
    FROM silver.erp_cust_az12
    WHERE bdate < '1924-01-01'
       OR bdate > GETDATE();


    /* =========================
       Data Standardization & Consistency
    ========================= */

    SELECT DISTINCT
        gen
    FROM bronze.erp_cust_az12;


    SELECT
        *
    FROM silver.erp_cust_az12;


    /* =========================
       Silver.erp_loc_a101
    ========================= */

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN c_clean = 'DE' THEN 'Germany'
            WHE

