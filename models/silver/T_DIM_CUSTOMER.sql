{{ config(
    materialized = 'incremental',
    unique_key = ['CUSTOMER_SK']
) }}

-- Step 1: Load new data from source using the provided field mappings
WITH source_data AS (
    SELECT
        CAST(TRIM(C0CST) AS NUMBER(10,0)) AS CUSTOMER_ID,
        CAST(TRIM(C0PCST) AS NUMBER(10,0)) AS PARENT_CUSTOMER_ID,
        CAST(TRIM(C0NAME) AS VARCHAR(30)) AS CUSTOMER_NAME,
        CAST(TRIM(C0ADR1) AS VARCHAR(30)) AS ADDRESS_LINE_1,
        CAST(TRIM(C0ADR2) AS VARCHAR(30)) AS ADDRESS_LINE_2,
        CAST(TRIM(C0ADR3) AS VARCHAR(30)) AS ADDRESS_LINE_3,
        CAST(TRIM(C0CITY) AS VARCHAR(30)) AS CITY,
        CAST(TRIM(C0STAT) AS VARCHAR(2)) AS STATE_CODE,
        CAST(TRIM(C0ZIP) AS VARCHAR(10)) AS ZIP_CODE,
        CAST(TRIM(C0CNNM) AS VARCHAR(30)) AS CONTACT_NAME,
        CAST(TRIM(C0CPHN) AS VARCHAR(10)) AS CONTACT_PHONE,
        CAST(TRIM(C0CPE1) AS VARCHAR(5)) AS CONTACT_PHONE_EXT,
        CAST(TRIM(C0CPH2) AS VARCHAR(10)) AS CONTACT_PHONE_2,
        CAST(TRIM(C0CPE2) AS VARCHAR(5)) AS CONTACT_PHONE_2_EXT,
        CAST(TRIM(C0CPH3) AS VARCHAR(10)) AS CONTACT_PHONE_3,
        CAST(TRIM(C0CPE3) AS VARCHAR(5)) AS CONTACT_PHONE_3_EXT,
        CAST(TRIM(C0CFAX) AS VARCHAR(10)) AS CONTACT_FAX,
        CAST(TRIM(C0CEML) AS VARCHAR(30)) AS CONTACT_EMAIL,
        CAST(TRIM(C0WEBS) AS VARCHAR(30)) AS WEBSITE,
        CAST(TRIM(C0INF1) AS VARCHAR(20)) AS INFO_1,
        CAST(TRIM(C0INF2) AS VARCHAR(20)) AS INFO_2,
        CAST(TRIM(C0INF3) AS VARCHAR(20)) AS INFO_3,
        CAST(TRIM(C0INF4) AS VARCHAR(20)) AS INFO_4,
        CAST(TRIM(C0INF5) AS VARCHAR(20)) AS INFO_5,
        CAST(TRIM(C0SRST) AS NUMBER(10,0)) AS SALES_REP_STORE,
        CAST(TRIM(C0SLRP) AS NUMBER(5,0)) AS SALES_REP_ID,
        CAST(TRIM(C0TXCD) AS NUMBER(5,0)) AS TAX_CODE,
        CAST(TRIM(C0PORQ) AS VARCHAR(1)) AS PURCHASE_ORDER_REQUIRED_FLAG,
        CAST(TRIM(C0DRRQ) AS VARCHAR(1)) AS DRIVERS_LICENSE_REQUIRED_FLAG,
        CAST(TRIM(C0AGCD) AS NUMBER(5,0)) AS AGENCY_CODE,
        CAST(TRIM(C0PRCD) AS NUMBER(5,0)) AS PRICE_CODE,
        CAST(TRIM(C0TMCD) AS NUMBER(5,0)) AS TERMS_CODE,
        CAST(TRIM(C0CRLM) AS NUMBER(7,0)) AS CREDIT_LIMIT,
        CAST(TRIM(C0BLCD) AS NUMBER(5,0)) AS BILLING_CODE,
        CAST(TRIM(C0TDCD) AS NUMBER(5,0)) AS TERMS_DISCOUNT_CODE,
        CAST(TRIM(C0TERR) AS NUMBER(5,0)) AS TERRITORY_ID,
        CAST(TRIM(C0STORE) AS NUMBER(5,0)) AS STORE_ID,
        CAST(TRIM(C0APBY) AS VARCHAR(3)) AS APPROVAL_BY,
        CAST(TRIM(C0ACTY) AS NUMBER(5,0)) AS ACCOUNT_TYPE,
        CAST(TRIM(C0ACDT) AS NUMBER(10,0)) AS ACCOUNT_TYPE_2,
        CAST(TRIM(C0SVCG) AS VARCHAR(1)) AS SERVICE_CHARGE_FLAG,
        CAST(TRIM(C0STMT) AS VARCHAR(1)) AS STATEMENT_FLAG,
        CAST(TRIM(C0ENDT) AS NUMBER(8,0)) AS ENTRY_DATE,
        CAST(TRIM(C0ECOF) AS VARCHAR(20)) AS C0ECOF,
        CAST(TRIM(C0PYMT) AS VARCHAR(1)) AS PAYMENT_FLAG,
        CAST(TRIM(C0AR) AS VARCHAR(1)) AS AR_FLAG,
        CAST(TRIM(C0PPTP) AS NUMBER(8,0)) AS PREFERRED_PAYMENT_TYPE,
        CAST(TRIM(C0RVCH) AS FLOAT) AS REVOLVING_CHARGE,
        CAST(TRIM(C0SFEX) AS VARCHAR(1)) AS STATE_TIRE_FEE_EXEMPT_FLAG,
        CAST(TRIM(C0EMST) AS NUMBER(3,0)) AS EMPLOYEE_STORE,
        CAST(TRIM(C0EMP) AS NUMBER(5,0)) AS EMPLOYEE_ID,
        CAST(TRIM(C0LSPD) AS NUMBER(8,0)) AS LAST_PAYMENT_DATE,
        CAST(TRIM(C0LSAM) AS FLOAT) AS LAST_PAYMENT_AMOUNT,
        CAST(TRIM(C0LSTP) AS VARCHAR(2)) AS LAST_PAYMENT_TYPE,
        CAST(TRIM(C0DBRT) AS VARCHAR(3)) AS DB_RATING,
        CAST(TRIM(C0DBDT) AS NUMBER(8,0)) AS DB_RATING_DATE,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(C0NAME), ''),
            COALESCE(TRIM(C0ADR1), ''),
            COALESCE(TRIM(C0ADR2), ''),
            COALESCE(TRIM(C0ADR3), ''),
            COALESCE(TRIM(C0CITY), ''),
            COALESCE(TRIM(C0STAT), ''),
            COALESCE(TRIM(C0ZIP), ''),
            COALESCE(TRIM(C0CNNM), ''),
            COALESCE(TRIM(C0CPHN), ''),
            COALESCE(TRIM(C0CPE1), ''),
            COALESCE(TRIM(C0CPH2), ''),
            COALESCE(TRIM(C0CPE2), ''),
            COALESCE(TRIM(C0CPH3), ''),
            COALESCE(TRIM(C0CPE3), ''),
            COALESCE(TRIM(C0CFAX), ''),
            COALESCE(TRIM(C0CEML), ''),
            COALESCE(TRIM(C0WEBS), ''),
            COALESCE(TRIM(C0INF1), ''),
            COALESCE(TRIM(C0INF2), ''),
            COALESCE(TRIM(C0INF3), ''),
            COALESCE(TRIM(C0INF4), ''),
            COALESCE(TRIM(C0INF5), ''),
            COALESCE(TRIM(C0SRST), ''),
            COALESCE(TRIM(C0SLRP), ''),
            COALESCE(TRIM(C0TXCD), ''),
            COALESCE(TRIM(C0PORQ), ''),
            COALESCE(TRIM(C0DRRQ), ''),
            COALESCE(TRIM(C0AGCD), ''),
            COALESCE(TRIM(C0PRCD), ''),
            COALESCE(TRIM(C0TMCD), ''),
            COALESCE(TRIM(C0CRLM), ''),
            COALESCE(TRIM(C0BLCD), ''),
            COALESCE(TRIM(C0TDCD), ''),
            COALESCE(TRIM(C0TERR), ''),
            COALESCE(TRIM(C0STORE), ''),
            COALESCE(TRIM(C0APBY), ''),
            COALESCE(TRIM(C0ACTY), ''),
            COALESCE(TRIM(C0ACDT), ''),
            COALESCE(TRIM(C0SVCG), ''),
            COALESCE(TRIM(C0STMT), ''),
            COALESCE(TRIM(C0ENDT), ''),
            COALESCE(TRIM(C0ECOF), ''),
            COALESCE(TRIM(C0PYMT), ''),
            COALESCE(TRIM(C0AR), ''),
            COALESCE(TRIM(C0PPTP), ''),
            COALESCE(TRIM(C0RVCH), ''),
            COALESCE(TRIM(C0SFEX), ''),
            COALESCE(TRIM(C0EMST), ''),
            COALESCE(TRIM(C0EMP), ''),
            COALESCE(TRIM(C0LSPD), ''),
            COALESCE(TRIM(C0LSAM), ''),
            COALESCE(TRIM(C0LSTP), ''),
            COALESCE(TRIM(C0DBRT), ''),
            COALESCE(TRIM(C0DBDT), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM {{ source('bronze_data', 'T_BRZ_CUSTOMER_MASTER_SACUST') }}
    {% if is_incremental() %}
        -- For incremental loads, adjust the condition as needed.
        WHERE ENTRY_TIMESTAMP = '1900-01-01T00:00:00Z'
    {% endif %}
),

-- Step 2: Deduplication by CUSTOMER_ID
ranked_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect inserts/updates (excluding soft deletes)
source_with_lag AS (
    SELECT
        curr.*,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY CUSTOMER_ID ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE (RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL)
      AND OPERATION != 'DELETE'
),

-- Step 3b: Detect soft deletes
deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

-- Step 4: Get maximum surrogate key from the target
max_key AS (
    SELECT COALESCE(MAX(CUSTOMER_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT
        *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.CUSTOMER_ID, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS CUSTOMER_SK,
        oc.CUSTOMER_ID,
        oc.PARENT_CUSTOMER_ID,
        oc.CUSTOMER_NAME,
        oc.ADDRESS_LINE_1,
        oc.ADDRESS_LINE_2,
        oc.ADDRESS_LINE_3,
        oc.CITY,
        oc.STATE_CODE,
        oc.ZIP_CODE,
        oc.CONTACT_NAME,
        oc.CONTACT_PHONE,
        oc.CONTACT_PHONE_EXT,
        oc.CONTACT_PHONE_2,
        oc.CONTACT_PHONE_2_EXT,
        oc.CONTACT_PHONE_3,
        oc.CONTACT_PHONE_3_EXT,
        oc.CONTACT_FAX,
        oc.CONTACT_EMAIL,
        oc.WEBSITE,
        oc.INFO_1,
        oc.INFO_2,
        oc.INFO_3,
        oc.INFO_4,
        oc.INFO_5,
        oc.SALES_REP_STORE,
        oc.SALES_REP_ID,
        oc.TAX_CODE,
        oc.PURCHASE_ORDER_REQUIRED_FLAG,
        oc.DRIVERS_LICENSE_REQUIRED_FLAG,
        oc.AGENCY_CODE,
        oc.PRICE_CODE,
        oc.TERMS_CODE,
        oc.CREDIT_LIMIT,
        oc.BILLING_CODE,
        oc.TERMS_DISCOUNT_CODE,
        oc.TERRITORY_ID,
        oc.STORE_ID,
        oc.APPROVAL_BY,
        oc.ACCOUNT_TYPE,
        oc.ACCOUNT_TYPE_2,
        oc.SERVICE_CHARGE_FLAG,
        oc.STATEMENT_FLAG,
        oc.ENTRY_DATE,
        oc.C0ECOF,
        oc.PAYMENT_FLAG,
        oc.AR_FLAG,
        oc.PREFERRED_PAYMENT_TYPE,
        oc.REVOLVING_CHARGE,
        oc.STATE_TIRE_FEE_EXEMPT_FLAG,
        oc.EMPLOYEE_STORE,
        oc.EMPLOYEE_ID,
        oc.LAST_PAYMENT_DATE,
        oc.LAST_PAYMENT_AMOUNT,
        oc.LAST_PAYMENT_TYPE,
        oc.DB_RATING,
        oc.DB_RATING_DATE,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE NULL
        END AS EXPIRATION_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS IS_CURRENT_FLAG,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        CURRENT_TIMESTAMP AS INGESTION_DTTM,
        CURRENT_DATE AS INGESTION_DT
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.CUSTOMER_ID = oc.CUSTOMER_ID
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old current rows when updates occur
expired_rows AS (
    SELECT
        old.CUSTOMER_SK,
        old.CUSTOMER_ID,
        old.PARENT_CUSTOMER_ID,
        old.CUSTOMER_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE_CODE,
        old.ZIP_CODE,
        old.CONTACT_NAME,
        old.CONTACT_PHONE,
        old.CONTACT_PHONE_EXT,
        old.CONTACT_PHONE_2,
        old.CONTACT_PHONE_2_EXT,
        old.CONTACT_PHONE_3,
        old.CONTACT_PHONE_3_EXT,
        old.CONTACT_FAX,
        old.CONTACT_EMAIL,
        old.WEBSITE,
        old.INFO_1,
        old.INFO_2,
        old.INFO_3,
        old.INFO_4,
        old.INFO_5,
        old.SALES_REP_STORE,
        old.SALES_REP_ID,
        old.TAX_CODE,
        old.PURCHASE_ORDER_REQUIRED_FLAG,
        old.DRIVERS_LICENSE_REQUIRED_FLAG,
        old.AGENCY_CODE,
        old.PRICE_CODE,
        old.TERMS_CODE,
        old.CREDIT_LIMIT,
        old.BILLING_CODE,
        old.TERMS_DISCOUNT_CODE,
        old.TERRITORY_ID,
        old.STORE_ID,
        old.APPROVAL_BY,
        old.ACCOUNT_TYPE,
        old.ACCOUNT_TYPE_2,
        old.SERVICE_CHARGE_FLAG,
        old.STATEMENT_FLAG,
        old.ENTRY_DATE,
        old.C0ECOF,
        old.PAYMENT_FLAG,
        old.AR_FLAG,
        old.PREFERRED_PAYMENT_TYPE,
        old.REVOLVING_CHARGE,
        old.STATE_TIRE_FEE_EXEMPT_FLAG,
        old.EMPLOYEE_STORE,
        old.EMPLOYEE_ID,
        old.LAST_PAYMENT_DATE,
        old.LAST_PAYMENT_AMOUNT,
        old.LAST_PAYMENT_TYPE,
        old.DB_RATING,
        old.DB_RATING_DATE,
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
      ON old.CUSTOMER_ID = new.CUSTOMER_ID
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes – expire records when the source OPERATION = 'DELETE'
soft_deleted_rows AS (
    SELECT
        old.CUSTOMER_SK,
        old.CUSTOMER_ID,
        old.PARENT_CUSTOMER_ID,
        old.CUSTOMER_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE_CODE,
        old.ZIP_CODE,
        old.CONTACT_NAME,
        old.CONTACT_PHONE,
        old.CONTACT_PHONE_EXT,
        old.CONTACT_PHONE_2,
        old.CONTACT_PHONE_2_EXT,
        old.CONTACT_PHONE_3,
        old.CONTACT_PHONE_3_EXT,
        old.CONTACT_FAX,
        old.CONTACT_EMAIL,
        old.WEBSITE,
        old.INFO_1,
        old.INFO_2,
        old.INFO_3,
        old.INFO_4,
        old.INFO_5,
        old.SALES_REP_STORE,
        old.SALES_REP_ID,
        old.TAX_CODE,
        old.PURCHASE_ORDER_REQUIRED_FLAG,
        old.DRIVERS_LICENSE_REQUIRED_FLAG,
        old.AGENCY_CODE,
        old.PRICE_CODE,
        old.TERMS_CODE,
        old.CREDIT_LIMIT,
        old.BILLING_CODE,
        old.TERMS_DISCOUNT_CODE,
        old.TERRITORY_ID,
        old.STORE_ID,
        old.APPROVAL_BY,
        old.ACCOUNT_TYPE,
        old.ACCOUNT_TYPE_2,
        old.SERVICE_CHARGE_FLAG,
        old.STATEMENT_FLAG,
        old.ENTRY_DATE,
        old.C0ECOF,
        old.PAYMENT_FLAG,
        old.AR_FLAG,
        old.PREFERRED_PAYMENT_TYPE,
        old.REVOLVING_CHARGE,
        old.STATE_TIRE_FEE_EXEMPT_FLAG,
        old.EMPLOYEE_STORE,
        old.EMPLOYEE_ID,
        old.LAST_PAYMENT_DATE,
        old.LAST_PAYMENT_AMOUNT,
        old.LAST_PAYMENT_TYPE,
        old.DB_RATING,
        old.DB_RATING_DATE,
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
      ON old.CUSTOMER_ID = del.CUSTOMER_ID
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final Output: Union of expired, soft-deleted, and new rows in the order of target columns
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deleted_rows
UNION ALL
SELECT * FROM new_rows
