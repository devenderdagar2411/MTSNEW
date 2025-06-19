{{ config(
    materialized = 'incremental',
    unique_key = ['WORK_ORDER_HEADER_SK']
) }}

WITH source_data AS (
    SELECT
        -- Key fields
        CAST(TRIM(W1STORE) AS NUMBER(3, 0)) AS STORE_NUMBER,
        CAST(TRIM(W1FMTP) AS VARCHAR(10)) AS FORM_TYPE_CODE,
        CAST(TRIM(W1WIPX) AS VARCHAR(10)) AS POS_PREFIX,
        CAST(TRIM(W1WO) AS NUMBER(10, 0)) AS WORK_ORDER_NUMBER,

        -- Date fields
        CAST(TRIM(W1TRDT) AS VARCHAR(8)) AS TRANSACTION_DATE,
        CAST(TRIM(W1OTDT) AS VARCHAR(8)) AS ORIGIN_TRANSACTION_DATE,
        TRY_TO_DATE(TRIM(W1PRDT), 'YYYYMMDD') AS PRINT_DT,
        CAST(TRIM(W1REGN) AS NUMBER(10, 0)) AS CASH_REGISTER_NUMBER,

        -- Dimension references
        CAST(TRIM(W1SRST) AS NUMBER(3, 0)) AS SALES_REP_STORE_NUMBER,
        CAST(TRIM(W1SLRP) AS NUMBER(10, 0)) AS SALES_REP_NUMBER,
        CAST(TRIM(W1CST) AS NUMBER(10, 0)) AS CUSTOMER_NUMBER,
        CAST(TRIM(W1NAVD) AS NUMBER(10, 0)) AS NATIONAL_ACCOUNT_VENDOR_NUMBER,
        CAST(TRIM(W1VSTS) AS VARCHAR(20)) AS VEHICLE_STATUS,
        CAST(TRIM(W1SHPN) AS NUMBER(10, 0)) AS SHIPPING_TO_NUMBER,

        -- Customer information
        CAST(TRIM(W1NAME) AS VARCHAR(100)) AS CUSTOMER_NAME,
        CAST(TRIM(W1ADR1) AS VARCHAR(100)) AS ADDRESS_LINE_1,
        CAST(TRIM(W1ADR2) AS VARCHAR(100)) AS ADDRESS_LINE_2,
        CAST(TRIM(W1ADR3) AS VARCHAR(100)) AS ADDRESS_LINE_3,
        CAST(TRIM(W1CITY) AS VARCHAR(50)) AS CITY,
        CAST(TRIM(W1STAT) AS VARCHAR(10)) AS STATE,
        CAST(TRIM(W1ZIP) AS VARCHAR(20)) AS ZIP_CODE,
        CAST(TRIM(W1WPHN) AS VARCHAR(20)) AS WORK_PHONE,
        CAST(TRIM(W1WPE1) AS VARCHAR(10)) AS WORK_PHONE_EXT,
        CAST(TRIM(W1HPHN) AS VARCHAR(20)) AS HOME_PHONE,
        CAST(TRIM(W1TMCD) AS NUMBER(3, 0)) AS TERMS_CODE,
        CAST(TRIM(W1TXCD) AS NUMBER(3, 0)) AS TAX_CODE,
        CAST(TRIM(W1TDCD) AS NUMBER(10, 0)) AS TAX_DISTRICT_CODE,
        CAST(TRIM(W1BLCD) AS NUMBER(3, 0)) AS BILLING_CODE,
        CAST(TRIM(W1DRNO) AS VARCHAR(20)) AS DRIVER_NUMBER,
        CAST(TRIM(W1PO) AS VARCHAR(50)) AS PURCHASE_ORDER_NUMBER,

        -- Shipping information
        CAST(TRIM(W1SHNM) AS VARCHAR(100)) AS SHIPPING_NAME,
        CAST(TRIM(W1SAD1) AS VARCHAR(100)) AS SHIP_ADDRESS_1,
        CAST(TRIM(W1SAD2) AS VARCHAR(100)) AS SHIP_ADDRESS_2,
        CAST(TRIM(W1SAD3) AS VARCHAR(100)) AS SHIP_ADDRESS_3,
        CAST(TRIM(W1SCTY) AS VARCHAR(50)) AS SHIP_CITY,
        CAST(TRIM(W1SSTA) AS VARCHAR(10)) AS SHIP_STATE,
        CAST(TRIM(W1SZIP) AS VARCHAR(20)) AS SHIP_ZIP,

        -- Origin information
        CAST(TRIM(W1OFMT) AS VARCHAR(10)) AS ORIGIN_FORM_TYPE_CODE,
        CAST(TRIM(W1OSRS) AS NUMBER(20, 0)) AS ORIGIN_SALES_REP_STORE_NUMBER,
        CAST(TRIM(W1OSLR) AS NUMBER(20, 0)) AS ORIGIN_SALES_REP_NUMBER,
        CAST(TRIM(W1STS) AS VARCHAR(20)) AS WORK_ORDER_STATUS,

        -- Flag fields
        CAST(TRIM(W1HOLD) AS VARCHAR(10)) AS HOLD_FLAG,
        CAST(TRIM(W1QSTS) AS VARCHAR(10)) AS QUESTION_ASKED_FLAG,
        CAST(TRIM(W1CSTS) AS VARCHAR(10)) AS COMMENT_FLAG,
        CAST(TRIM(W1AR) AS VARCHAR(10)) AS ACCT_REVERSAL_FLAG,
        CAST(TRIM(W1PYMT) AS VARCHAR(10)) AS PAYMENT_FLAG,
        CAST(TRIM(W1PYNM) AS VARCHAR(50)) AS PAYMENT_TYPE_NAME,

        -- Fact fields (measures)
        CAST(TRIM(W1TEXM) AS NUMERIC(12,2)) AS TOTAL_TAX_EXEMPT_AMT,
        CAST(TRIM(W1TTXB) AS NUMERIC(12,2)) AS TOTAL_TAXABLE_AMT,
        CAST(TRIM(W1TSLT) AS NUMERIC(12,2)) AS SALES_SALES_TAX_AMT,
        CAST(TRIM(W1TAMT) AS NUMERIC(12,2)) AS TOTAL_AMOUNT,
        CAST(TRIM(W1AMT) AS NUMERIC(12,2)) AS TOTAL_EXTENDED_AMT,
        CAST(TRIM(W1CAMT) AS NUMERIC(12,2)) AS TOTAL_EXTENDED_COST_AMT,
        CAST(TRIM(W1AMTGP) AS NUMERIC(12,2)) AS TOTAL_EXTENDED_GP_AMT,
        CAST(TRIM(W1CAMTGP) AS NUMERIC(12,2)) AS TOTAL_EXTENDED_COST_GP_AMT,
        CAST(TRIM(W1GPAM) AS NUMERIC(12,2)) AS TOTAL_GP_AMT,
        CAST(TRIM(W1GPMG) AS NUMERIC(9,2)) AS TOTAL_PROFIT_MARGIN,
        CAST(TRIM(W1GPNM) AS NUMERIC(12,2)) AS TOTAL_GP_NAB_AMT,
        CAST(TRIM(W1GPNG) AS NUMERIC(9,2)) AS TOTAL_NAB_PROFIT_MARGIN,
        CAST(TRIM(W1TSAM) AS NUMERIC(12,2)) AS TOTAL_SPIFF_AMT,
        CAST(TRIM(W1TSPT) AS NUMBER(3, 0)) AS TOTAL_SPIFF_POINTS,
        CAST(TRIM(W1TFEE) AS NUMERIC(12,2)) AS TOTAL_TIRE_FEE,
        CAST(TRIM(W1PRSQ) AS NUMBER(10, 0)) AS NUMBER_OF_TIMES_PRINTED,
        CAST(TRIM(W1NACD) AS NUMBER(9,2)) AS NAB_CREDIT_DUE,
        CAST(TRIM(W1NACR) AS NUMERIC(9,2)) AS NAB_CREDIT_RECEIVED,
        CAST(TRIM(W1NACP) AS NUMBER(9,2)) AS NAB_CREDIT_PENDING,

        -- Additional dimension fields
        CAST(TRIM(W1ARST) AS NUMERIC(20, 0)) AS CREDITED_SALES_REP_STORE_NUMBER,
        CAST(TRIM(W1ARRP) AS NUMERIC(20, 0)) AS CREDITED_SALES_REP_NUMBER,
        CAST(TRIM(W1STOREO) AS NUMERIC(20, 0)) AS ORIGIN_INVOICED_STORE_NUMBER,
        CAST(TRIM(W1FMTPO) AS VARCHAR(10)) AS ORIGIN_INVOICED_FORM_TYPE_CODE,
        CAST(TRIM(W1WIPXO) AS VARCHAR(10)) AS ORIGIN_INVOICED_POS_PREFIX,
        CAST(TRIM(W1WOO) AS NUMERIC(10, 0)) AS ORIGIN_INVOICED_WORK_ORDER_NUMBER,
        CAST(TRIM(W1CPAT) AS VARCHAR(10)) AS CORP_AUTH_ASKED_FLAG,
    -- Audit columns
    CAST(TRIM(base.SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
    CAST(TRIM(base.SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
    CAST(TRIM(base.BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
    CAST(TRIM(base.ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
    CAST(TRIM(base.OPERATION) AS VARCHAR(10)) AS OPERATION,   
    TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_HEADER_WOMSTH') }} base
    {% if is_incremental() %}
    WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CONCAT_WS('|', 
            CAST(TRIM(W1STORE) AS NUMBER(3, 0)), 
            CAST(TRIM(W1FMTP) AS VARCHAR(10)), 
            CAST(TRIM(W1WIPX) AS VARCHAR(10)), 
            CAST(TRIM(W1WO) AS NUMBER(10, 0))
        )
        ORDER BY TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) DESC,W1CYMD desc,
    W1HMS desc
    ) = 1
),

-- Step 2: Remove the source_with_keys CTE since joins are now in source_data
-- Step 3: Rank records by business key to handle duplicates
ranked_source AS (
    SELECT
        sd.*,
        
        -- Surrogate keys from joins
        dt.DATE_KEY AS TRANSACTION_DATE_SK,
        odt.DATE_KEY AS ORIGIN_TRANSACTION_SK,
        sm.STORE_MANAGER_SK as STORE_MANAGER_SK,
        srst.STORE_SK AS SALES_REP_STORE_SK,
        sr.SALES_REP_SK,
        c.CUSTOMER_SK,
        -- Additional surrogate keys for form types
        ft.FORM_TYPE_SK AS ORIGIN_FORM_TYPE_SK,
        ift.FORM_TYPE_SK AS ORIGIN_INVOICED_FORM_TYPE_SK,
        -- Additional surrogate keys for stores
        orst.STORE_SK AS ORIGIN_SALES_REP_STORE_SK,
        crst.STORE_SK AS CREDITED_SALES_REP_STORE_SK,
        ist.STORE_SK AS ORIGIN_INVOICED_STORE_SK,
        -- Additional surrogate keys for sales reps
        orsr.SALES_REP_SK AS ORIGIN_SALES_REP_SK,
        crsr.SALES_REP_SK AS CREDITED_SALES_REP_SK,
        
        -- Create a tracking hash for change detection including all source fields AND joined surrogate keys
        MD5(CONCAT_WS('|',
            -- Original source fields
            COALESCE(sd.TRANSACTION_DATE, ''),
            COALESCE(sd.ORIGIN_TRANSACTION_DATE, ''),
            COALESCE(CAST(sd.PRINT_DT AS VARCHAR), ''),
            COALESCE(CAST(sd.CASH_REGISTER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.SALES_REP_STORE_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.SALES_REP_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.CUSTOMER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.NATIONAL_ACCOUNT_VENDOR_NUMBER AS VARCHAR), ''),
            COALESCE(sd.VEHICLE_STATUS, ''),
            COALESCE(CAST(sd.SHIPPING_TO_NUMBER AS VARCHAR), ''),
            COALESCE(sd.CUSTOMER_NAME, ''),
            COALESCE(sd.ADDRESS_LINE_1, ''),
            COALESCE(sd.ADDRESS_LINE_2, ''),
            COALESCE(sd.ADDRESS_LINE_3, ''),
            COALESCE(sd.CITY, ''),
            COALESCE(sd.STATE, ''),
            COALESCE(sd.ZIP_CODE, ''),
            COALESCE(sd.WORK_PHONE, ''),
            COALESCE(sd.WORK_PHONE_EXT, ''),
            COALESCE(sd.HOME_PHONE, ''),
            COALESCE(CAST(sd.TERMS_CODE AS VARCHAR), ''),
            COALESCE(CAST(sd.TAX_CODE AS VARCHAR), ''),
            COALESCE(CAST(sd.TAX_DISTRICT_CODE AS VARCHAR), ''),
            COALESCE(CAST(sd.BILLING_CODE AS VARCHAR), ''),
            COALESCE(sd.DRIVER_NUMBER, ''),
            COALESCE(sd.PURCHASE_ORDER_NUMBER, ''),
            COALESCE(sd.SHIPPING_NAME, ''),
            COALESCE(sd.SHIP_ADDRESS_1, ''),
            COALESCE(sd.SHIP_ADDRESS_2, ''),
            COALESCE(sd.SHIP_ADDRESS_3, ''),
            COALESCE(sd.SHIP_CITY, ''),
            COALESCE(sd.SHIP_STATE, ''),
            COALESCE(sd.SHIP_ZIP, ''),
            COALESCE(sd.ORIGIN_FORM_TYPE_CODE, ''),
            COALESCE(CAST(sd.ORIGIN_SALES_REP_STORE_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.ORIGIN_SALES_REP_NUMBER AS VARCHAR), ''),
            COALESCE(sd.WORK_ORDER_STATUS, ''),
            COALESCE(sd.HOLD_FLAG, ''),
            COALESCE(sd.QUESTION_ASKED_FLAG, ''),
            COALESCE(sd.COMMENT_FLAG, ''),
            COALESCE(sd.ACCT_REVERSAL_FLAG, ''),
            COALESCE(sd.PAYMENT_FLAG, ''),
            COALESCE(sd.PAYMENT_TYPE_NAME, ''),
            COALESCE(CAST(sd.TOTAL_TAX_EXEMPT_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_TAXABLE_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.SALES_SALES_TAX_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_AMOUNT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_EXTENDED_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_EXTENDED_COST_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_EXTENDED_GP_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_EXTENDED_COST_GP_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_GP_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_PROFIT_MARGIN AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_GP_NAB_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_NAB_PROFIT_MARGIN AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_SPIFF_AMT AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_SPIFF_POINTS AS VARCHAR), ''),
            COALESCE(CAST(sd.TOTAL_TIRE_FEE AS VARCHAR), ''),
            COALESCE(CAST(sd.NUMBER_OF_TIMES_PRINTED AS VARCHAR), ''),
            COALESCE(CAST(sd.NAB_CREDIT_DUE AS VARCHAR), ''),
            COALESCE(CAST(sd.NAB_CREDIT_RECEIVED AS VARCHAR), ''),
            COALESCE(CAST(sd.NAB_CREDIT_PENDING AS VARCHAR), ''),
            COALESCE(CAST(sd.CREDITED_SALES_REP_STORE_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.CREDITED_SALES_REP_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.ORIGIN_INVOICED_STORE_NUMBER AS VARCHAR), ''),
            COALESCE(sd.ORIGIN_INVOICED_FORM_TYPE_CODE, ''),
            COALESCE(sd.ORIGIN_INVOICED_POS_PREFIX, ''),
            COALESCE(CAST(sd.ORIGIN_INVOICED_WORK_ORDER_NUMBER AS VARCHAR), ''),
            COALESCE(sd.CORP_AUTH_ASKED_FLAG, ''),
            -- Surrogate keys from dimension joins
            COALESCE(CAST(dt.DATE_KEY AS VARCHAR), ''),
            COALESCE(CAST(odt.DATE_KEY AS VARCHAR), ''),
            COALESCE(CAST(sm.STORE_MANAGER_SK AS VARCHAR), ''),
            COALESCE(CAST(srst.STORE_SK AS VARCHAR), ''),
            COALESCE(CAST(sr.SALES_REP_SK AS VARCHAR), ''),
            COALESCE(CAST(c.CUSTOMER_SK AS VARCHAR), ''),
            COALESCE(CAST(ft.FORM_TYPE_SK AS VARCHAR), ''),
            COALESCE(CAST(ift.FORM_TYPE_SK AS VARCHAR), ''),
            COALESCE(CAST(orst.STORE_SK AS VARCHAR), ''),
            COALESCE(CAST(crst.STORE_SK AS VARCHAR), ''),
            COALESCE(CAST(ist.STORE_SK AS VARCHAR), ''),
            COALESCE(CAST(orsr.SALES_REP_SK AS VARCHAR), ''),
            COALESCE(CAST(crsr.SALES_REP_SK AS VARCHAR), '')
        )) AS RECORD_CHECKSUM_HASH

    FROM source_data sd
    -- Date dimension lookups
    LEFT JOIN {{ source('silver_data', 'T_DIM_DATE') }} dt ON dt.DATE_KEY = sd.TRANSACTION_DATE
    LEFT JOIN {{ source('silver_data', 'T_DIM_DATE') }} odt ON odt.DATE_KEY = sd.ORIGIN_TRANSACTION_DATE

    --StoreManager dimension lookup
    LEFT JOIN {{ ref('T_DIM_STORE_MANAGER') }} sm ON sm.STORE_NUMBER = sd.SALES_REP_STORE_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN sm.EFFECTIVE_DATE AND COALESCE(sm.EXPIRATION_DATE, '9999-12-31')
    
    -- Store dimension lookups
    LEFT JOIN {{ ref('T_DIM_STORE') }} srst ON srst.STORE_NUMBER = sd.SALES_REP_STORE_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN srst.EFFECTIVE_DATE AND COALESCE(srst.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_STORE') }} orst ON orst.STORE_NUMBER = sd.ORIGIN_SALES_REP_STORE_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN orst.EFFECTIVE_DATE AND COALESCE(orst.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_STORE') }} crst ON crst.STORE_NUMBER = sd.CREDITED_SALES_REP_STORE_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN crst.EFFECTIVE_DATE AND COALESCE(crst.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_STORE') }} ist ON ist.STORE_NUMBER = sd.ORIGIN_INVOICED_STORE_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN ist.EFFECTIVE_DATE AND COALESCE(ist.EXPIRATION_DATE, '9999-12-31')
    
    -- Sales rep dimension lookups
    LEFT JOIN {{ ref('T_DIM_SALES_REP') }} sr ON sr.SALES_REP_NUMBER = sd.SALES_REP_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN sr.EFFECTIVE_DATE AND COALESCE(sr.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_SALES_REP') }} orsr ON orsr.SALES_REP_NUMBER = sd.ORIGIN_SALES_REP_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN orsr.EFFECTIVE_DATE AND COALESCE(orsr.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_SALES_REP') }} crsr ON crsr.SALES_REP_NUMBER = sd.CREDITED_SALES_REP_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN crsr.EFFECTIVE_DATE AND COALESCE(crsr.EXPIRATION_DATE, '9999-12-31')
    
    -- Form type dimension lookups
    LEFT JOIN {{ ref('T_DIM_FORM_TYPE') }} ft ON ft.FORM_TYPE_CODE = sd.ORIGIN_FORM_TYPE_CODE 
        AND sd.ENTRY_TIMESTAMP BETWEEN ft.EFFECTIVE_DATE AND COALESCE(ft.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ ref('T_DIM_FORM_TYPE') }} ift ON ift.FORM_TYPE_CODE = sd.ORIGIN_INVOICED_FORM_TYPE_CODE 
        AND sd.ENTRY_TIMESTAMP BETWEEN ift.EFFECTIVE_DATE AND COALESCE(ift.EXPIRATION_DATE, '9999-12-31')
    
    -- Customer dimension lookup
    LEFT JOIN {{ ref('T_DIM_CUSTOMER') }} c ON c.CUSTOMER_ID = sd.CUSTOMER_NUMBER 
        AND sd.ENTRY_TIMESTAMP BETWEEN c.EFFECTIVE_DATE AND COALESCE(c.EXPIRATION_DATE, '9999-12-31')
),

-- Step 4: Remove duplicates by taking the latest record for each business key
-- Step 7: Identify deleted records
deletes AS (
    SELECT *
    FROM ranked_source
    WHERE OPERATION = 'DELETE'
),

-- Step 8: Get the maximum existing surrogate key
max_key AS (
    SELECT COALESCE(MAX(WORK_ORDER_HEADER_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 9: Order changes and prepare for surrogate key assignment
ordered_changes AS (
    SELECT *
    FROM ranked_source
    WHERE OPERATION IN ('INSERT', 'UPDATE')
       
),

-- Step 10: Generate new records with surrogate keys
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY CONCAT_WS('|', oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER), oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS WORK_ORDER_HEADER_SK,
        oc.STORE_NUMBER,
        oc.FORM_TYPE_CODE,
        oc.POS_PREFIX,
        oc.WORK_ORDER_NUMBER,
        oc.TRANSACTION_DATE_SK,
        oc.ORIGIN_TRANSACTION_SK,
        oc.PRINT_DT,
        oc.CASH_REGISTER_NUMBER,
        oc.STORE_MANAGER_SK,
        oc.SALES_REP_STORE_SK,
        oc.SALES_REP_SK,
        oc.CUSTOMER_SK,
        oc.NATIONAL_ACCOUNT_VENDOR_NUMBER,
        oc.VEHICLE_STATUS,
        oc.SHIPPING_TO_NUMBER,
        oc.CUSTOMER_NAME,
        oc.ADDRESS_LINE_1,
        oc.ADDRESS_LINE_2,
        oc.ADDRESS_LINE_3,
        oc.CITY,
        oc.STATE,
        oc.ZIP_CODE,
        oc.WORK_PHONE,
        oc.WORK_PHONE_EXT,
        oc.HOME_PHONE,
        oc.TERMS_CODE,
        oc.TAX_CODE,
        oc.TAX_DISTRICT_CODE,
        oc.BILLING_CODE,
        oc.DRIVER_NUMBER,
        oc.PURCHASE_ORDER_NUMBER,
        oc.SHIPPING_NAME,
        oc.SHIP_ADDRESS_1,
        oc.SHIP_ADDRESS_2,
        oc.SHIP_ADDRESS_3,
        oc.SHIP_CITY,
        oc.SHIP_STATE,
        oc.SHIP_ZIP,
        CAST(oc.ORIGIN_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_FORM_TYPE_SK,
        oc.ORIGIN_SALES_REP_STORE_SK,
        oc.ORIGIN_SALES_REP_SK,
        oc.WORK_ORDER_STATUS,
        oc.HOLD_FLAG,
        oc.QUESTION_ASKED_FLAG,
        oc.COMMENT_FLAG,
        oc.ACCT_REVERSAL_FLAG ,
        oc.PAYMENT_FLAG,
        oc.PAYMENT_TYPE_NAME,
        oc.TOTAL_TAX_EXEMPT_AMT,
        oc.TOTAL_TAXABLE_AMT,
        oc.SALES_SALES_TAX_AMT,
        oc.TOTAL_AMOUNT,
        oc.TOTAL_EXTENDED_AMT,
        oc.TOTAL_EXTENDED_COST_AMT,
        oc.TOTAL_EXTENDED_GP_AMT,
        oc.TOTAL_EXTENDED_COST_GP_AMT,
        oc.TOTAL_GP_AMT,
        oc.TOTAL_PROFIT_MARGIN,
        oc.TOTAL_GP_NAB_AMT,
        oc.TOTAL_NAB_PROFIT_MARGIN,
        oc.TOTAL_SPIFF_AMT,
        oc.TOTAL_SPIFF_POINTS,
        oc.TOTAL_TIRE_FEE,
        oc.NUMBER_OF_TIMES_PRINTED,
        oc.NAB_CREDIT_DUE,
        oc.NAB_CREDIT_RECEIVED,
        oc.NAB_CREDIT_PENDING,
        oc.CREDITED_SALES_REP_STORE_SK,
        oc.CREDITED_SALES_REP_SK,
        oc.ORIGIN_INVOICED_STORE_SK,
        CAST(oc.ORIGIN_INVOICED_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_INVOICED_FORM_TYPE_SK,
        oc.ORIGIN_INVOICED_POS_PREFIX,
        oc.ORIGIN_INVOICED_WORK_ORDER_NUMBER,
        oc.CORP_AUTH_ASKED_FLAG,
        case when ENTRY_TIMESTAMP='1900-01-01T00:00:00Z' then TRY_TO_DATE(oc.ORIGIN_TRANSACTION_DATE ,'YYYYMMDD') ELSE ENTRY_TIMESTAMP END AS EFFECTIVE_DATE,
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS EXPIRATION_DATE,
        TRUE AS IS_CURRENT_FLAG,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
          FROM {{ this }} tgt
        WHERE CONCAT_WS('|', tgt.STORE_NUMBER, tgt.FORM_TYPE_CODE, tgt.POS_PREFIX, tgt.WORK_ORDER_NUMBER) = 
              CONCAT_WS('|', oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER)
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 11: Identify records to expire (current records that have changed)
expired_rows AS (
    SELECT
        old.WORK_ORDER_HEADER_SK,
        old.STORE_NUMBER,
        old.FORM_TYPE_CODE,
        old.POS_PREFIX,
        old.WORK_ORDER_NUMBER,
        old.TRANSACTION_DATE_SK,
        old.ORIGIN_TRANSACTION_SK,
        old.PRINT_DT,
        old.CASH_REGISTER_NUMBER,
        old.STORE_MANAGER_SK,
        old.SALES_REP_STORE_SK,
        old.SALES_REP_SK,
        old.CUSTOMER_SK,
        old.NATIONAL_ACCOUNT_VENDOR_NUMBER,
        old.VEHICLE_STATUS,
        old.SHIPPING_TO_NUMBER,
        old.CUSTOMER_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.WORK_PHONE,
        old.WORK_PHONE_EXT,
        old.HOME_PHONE,
        old.TERMS_CODE,
        old.TAX_CODE,
        old.TAX_DISTRICT_CODE,
        old.BILLING_CODE,
        old.DRIVER_NUMBER,
        old.PURCHASE_ORDER_NUMBER,
        old.SHIPPING_NAME,
        old.SHIP_ADDRESS_1,
        old.SHIP_ADDRESS_2,
        old.SHIP_ADDRESS_3,
        old.SHIP_CITY,
        old.SHIP_STATE,
        old.SHIP_ZIP,
        CAST(old.ORIGIN_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_FORM_TYPE_SK,
        old.ORIGIN_SALES_REP_STORE_SK,
        old.ORIGIN_SALES_REP_SK,
        old.WORK_ORDER_STATUS,
        old.HOLD_FLAG,
        old.QUESTION_ASKED_FLAG,
        old.COMMENT_FLAG,
        old.ACCT_REVERSAL_FLAG,
        old.PAYMENT_FLAG,
        old.PAYMENT_TYPE_NAME,
        old.TOTAL_TAX_EXEMPT_AMT,
        old.TOTAL_TAXABLE_AMT,
        old.SALES_SALES_TAX_AMT,
        old.TOTAL_AMOUNT,
        old.TOTAL_EXTENDED_AMT,
        old.TOTAL_EXTENDED_COST_AMT,
        old.TOTAL_EXTENDED_GP_AMT,
        old.TOTAL_EXTENDED_COST_GP_AMT,
        old.TOTAL_GP_AMT,
        old.TOTAL_PROFIT_MARGIN,
        old.TOTAL_GP_NAB_AMT,
        old.TOTAL_NAB_PROFIT_MARGIN,
        old.TOTAL_SPIFF_AMT,
        old.TOTAL_SPIFF_POINTS,
        old.TOTAL_TIRE_FEE,
        old.NUMBER_OF_TIMES_PRINTED,
        old.NAB_CREDIT_DUE,
        old.NAB_CREDIT_RECEIVED,
        old.NAB_CREDIT_PENDING,
        old.CREDITED_SALES_REP_STORE_SK,
        old.CREDITED_SALES_REP_SK,
        old.ORIGIN_INVOICED_STORE_SK,  -- Add this missing column
        CAST(old.ORIGIN_INVOICED_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_INVOICED_FORM_TYPE_SK,
        old.ORIGIN_INVOICED_POS_PREFIX,
        old.ORIGIN_INVOICED_WORK_ORDER_NUMBER,
        old.CORP_AUTH_ASKED_FLAG,
        old.EFFECTIVE_DATE,
        new.EFFECTIVE_DATE - INTERVAL '1 second' AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM_HASH,
        old.ETL_VERSION,
        old.INGESTION_DTTM,
        old.INGESTION_DT
     FROM {{ this }} old
    JOIN new_rows new
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER) = 
         CONCAT_WS('|', new.STORE_NUMBER, new.FORM_TYPE_CODE, new.POS_PREFIX, new.WORK_ORDER_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),
-- Step 12: Identify records to soft delete
soft_deletes AS (
    SELECT
        old.WORK_ORDER_HEADER_SK,
        old.STORE_NUMBER,
        old.FORM_TYPE_CODE,
        old.POS_PREFIX,
        old.WORK_ORDER_NUMBER,
        old.TRANSACTION_DATE_SK,
        old.ORIGIN_TRANSACTION_SK,
        old.PRINT_DT,
        old.CASH_REGISTER_NUMBER,
        old.STORE_MANAGER_SK,
        old.SALES_REP_STORE_SK,
        old.SALES_REP_SK,
        old.CUSTOMER_SK,
        old.NATIONAL_ACCOUNT_VENDOR_NUMBER,
        old.VEHICLE_STATUS,
        old.SHIPPING_TO_NUMBER,
        old.CUSTOMER_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.WORK_PHONE,
        old.WORK_PHONE_EXT,
        old.HOME_PHONE,
        old.TERMS_CODE,
        old.TAX_CODE,
        old.TAX_DISTRICT_CODE,
        old.BILLING_CODE,
        old.DRIVER_NUMBER,
        old.PURCHASE_ORDER_NUMBER,
        old.SHIPPING_NAME,
        old.SHIP_ADDRESS_1,
        old.SHIP_ADDRESS_2,
        old.SHIP_ADDRESS_3,
        old.SHIP_CITY,
        old.SHIP_STATE,
        old.SHIP_ZIP,       
        CAST(old.ORIGIN_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_FORM_TYPE_SK,
        old.ORIGIN_SALES_REP_STORE_SK,
        old.ORIGIN_SALES_REP_SK,
        old.WORK_ORDER_STATUS,
        old.HOLD_FLAG,
        old.QUESTION_ASKED_FLAG,
        old.COMMENT_FLAG,
        old.ACCT_REVERSAL_FLAG,
        old.PAYMENT_FLAG,
        old.PAYMENT_TYPE_NAME,
        old.TOTAL_TAX_EXEMPT_AMT,
        old.TOTAL_TAXABLE_AMT,
        old.SALES_SALES_TAX_AMT,
        old.TOTAL_AMOUNT,
        old.TOTAL_EXTENDED_AMT,
        old.TOTAL_EXTENDED_COST_AMT,
        old.TOTAL_EXTENDED_GP_AMT,
        old.TOTAL_EXTENDED_COST_GP_AMT,
        old.TOTAL_GP_AMT,
        old.TOTAL_PROFIT_MARGIN,
        old.TOTAL_GP_NAB_AMT,
        old.TOTAL_NAB_PROFIT_MARGIN,
        old.TOTAL_SPIFF_AMT,
        old.TOTAL_SPIFF_POINTS,
        old.TOTAL_TIRE_FEE,
        old.NUMBER_OF_TIMES_PRINTED,
        old.NAB_CREDIT_DUE,
        old.NAB_CREDIT_RECEIVED,
        old.NAB_CREDIT_PENDING,
        old.CREDITED_SALES_REP_STORE_SK,
        old.CREDITED_SALES_REP_SK,
        old.ORIGIN_INVOICED_STORE_SK,
        CAST(old.ORIGIN_INVOICED_FORM_TYPE_SK AS NUMBER(20,0)) AS ORIGIN_INVOICED_FORM_TYPE_SK,
        old.ORIGIN_INVOICED_POS_PREFIX,
        old.ORIGIN_INVOICED_WORK_ORDER_NUMBER,
        old.CORP_AUTH_ASKED_FLAG,
        old.EFFECTIVE_DATE,
        del.ENTRY_TIMESTAMP AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM_HASH,
        old.ETL_VERSION,
        old.INGESTION_DTTM,
        old.INGESTION_DT
     FROM {{ this }} old
    JOIN deletes del
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER) = 
         CONCAT_WS('|', del.STORE_NUMBER, del.FORM_TYPE_CODE, del.POS_PREFIX, del.WORK_ORDER_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
)

select * from new_rows
UNION ALL
select * from expired_rows
UNION ALL
select * FROM soft_deletes