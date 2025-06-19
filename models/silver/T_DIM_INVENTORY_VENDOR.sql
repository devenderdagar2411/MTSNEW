{{ config(
    materialized = 'incremental',
    unique_key = ['INVENTORY_VENDOR_SK']
) }}

-- Step 1: Load new data from source
WITH source_data AS (
    SELECT
        CAST(TRIM(AWIVDN) AS NUMERIC(6, 0)) AS VENDOR_NUMBER,
        CAST(TRIM(AWNAME) AS CHAR(40)) AS VENDOR_NAME,
        CAST(TRIM(AWADR1) AS CHAR(30)) AS ADDRESS_LINE_1,
        CAST(TRIM(AWADR2) AS CHAR(30)) AS ADDRESS_LINE_2,
        CAST(TRIM(AWADR3) AS CHAR(30)) AS ADDRESS_LINE_3,
        CAST(TRIM(AWCITY) AS CHAR(20)) AS CITY,
        CAST(TRIM(AWSTAT) AS CHAR(2)) AS STATE,
        CAST(TRIM(AWZIP) AS CHAR(9)) AS ZIP_CODE,
        CAST(TRIM(AWCPHN) AS CHAR(10)) AS CONTACT_PHONE1,
        CAST(TRIM(AWCSDS) AS NUMERIC(5, 4)) AS CASH_DISCOUNT,
        CAST(TRIM(AWAVDN) AS NUMERIC(6, 0)) AS AP_VENDOR_NUMBER,
        CAST(TRIM(AWAPPF) AS CHAR(1)) AS AP_PENDING_FILE,
        CAST(TRIM(AWVSTM) AS CHAR(1)) AS VENDOR_STATEMENT,
        CAST(TRIM(AWBOAL) AS CHAR(1)) AS BACK_ORDER_ALLOWED,
        CAST(TRIM(AWPOTM) AS NUMERIC(3, 0)) AS PO_TERMS,
        CAST(TRIM(AWFLTM) AS NUMERIC(3, 0)) AS FILL_IN_TERMS,
        CAST(TRIM(AWSTCU) AS NUMERIC(2, 0)) AS STMNT_CUT_OFF,
        CAST(TRIM(AWCRPL) AS NUMERIC(8, 0)) AS CURR_PRICE_LIST_DATE,
        CAST(TRIM(AWNAVD) AS CHAR(1)) AS NAB_VENDOR,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(AWNAME), ''),
            COALESCE(TRIM(AWADR1), ''),
            COALESCE(TRIM(AWCITY), ''),
            COALESCE(TRIM(AWSTAT), ''),
            COALESCE(TRIM(AWCPHN), ''),
            COALESCE(TRIM(AWCSDS), ''),
            COALESCE(TRIM(AWAVDN), ''),
            COALESCE(TRIM(AWAPPF), ''),
            COALESCE(TRIM(AWVSTM), ''),
            COALESCE(TRIM(AWBOAL), ''),
            COALESCE(TRIM(AWPOTM), ''),
            COALESCE(TRIM(AWFLTM), ''),
            COALESCE(TRIM(AWSTCU), ''),
            COALESCE(TRIM(AWCRPL), ''),
            COALESCE(TRIM(AWNAVD), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM {{ source('bronze_data', 'T_BRZ_INV_VENDOR_INVEND') }}
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }})
    {% endif %}
),

-- Step 2: Deduplication
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY VENDOR_NUMBER ORDER BY ENTRY_TIMESTAMP DESC) AS rn
    FROM source_data
),
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect inserts/updates
source_with_lag AS (
    SELECT *,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY VENDOR_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source
),
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE (RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL)
      AND OPERATION != 'DELETE'
),

-- Step 3b: Detect deletes
deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

-- Step 4: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(INVENTORY_VENDOR_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate expiration dates
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY VENDOR_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: New SCD2 rows
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY VENDOR_NUMBER, ENTRY_TIMESTAMP
        ) + max_key.max_sk AS INVENTORY_VENDOR_SK,
        oc.VENDOR_NUMBER,
        oc.VENDOR_NAME,
        oc.ADDRESS_LINE_1,
        oc.ADDRESS_LINE_2,
        oc.ADDRESS_LINE_3,
        oc.CITY,
        oc.STATE,
        oc.ZIP_CODE,
        oc.CONTACT_PHONE1,
        oc.CASH_DISCOUNT,
        oc.AP_VENDOR_NUMBER,
        oc.AP_PENDING_FILE,
        oc.VENDOR_STATEMENT,
        oc.BACK_ORDER_ALLOWED,
        oc.PO_TERMS,
        oc.FILL_IN_TERMS,
        oc.STMNT_CUT_OFF,
        oc.CURR_PRICE_LIST_DATE,
        oc.NAB_VENDOR,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE '9999-12-31 23:59:59'::TIMESTAMP_NTZ
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
        WHERE tgt.VENDOR_NUMBER = oc.VENDOR_NUMBER
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old rows
expired_rows AS (
    SELECT
        old.INVENTORY_VENDOR_SK,
        old.VENDOR_NUMBER,
        old.VENDOR_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.CONTACT_PHONE1,
        old.CASH_DISCOUNT,
        old.AP_VENDOR_NUMBER,
        old.AP_PENDING_FILE,
        old.VENDOR_STATEMENT,
        old.BACK_ORDER_ALLOWED,
        old.PO_TERMS,
        old.FILL_IN_TERMS,
        old.STMNT_CUT_OFF,
        old.CURR_PRICE_LIST_DATE,
        old.NAB_VENDOR,
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
      ON old.VENDOR_NUMBER = new.VENDOR_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes
soft_deleted_rows AS (
    SELECT
        old.INVENTORY_VENDOR_SK,
        old.VENDOR_NUMBER,
        old.VENDOR_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.CONTACT_PHONE1,
        old.CASH_DISCOUNT,
        old.AP_VENDOR_NUMBER,
        old.AP_PENDING_FILE,
        old.VENDOR_STATEMENT,
        old.BACK_ORDER_ALLOWED,
        old.PO_TERMS,
        old.FILL_IN_TERMS,
        old.STMNT_CUT_OFF,
        old.CURR_PRICE_LIST_DATE,
        old.NAB_VENDOR,
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
      ON old.VENDOR_NUMBER = del.VENDOR_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final Output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows
