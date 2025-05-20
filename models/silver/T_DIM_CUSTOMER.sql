{{ config(
    materialized = 'incremental',
    unique_key = ['CUSTOMER_SK']
) }}

-- Step 1: Load new data from source
WITH source_data AS (
    SELECT
        CAST(TRIM(C0CST) AS NUMBER(5, 0)) AS CUSTOMER_ID,
        CAST(TRIM(C0PCST) AS NUMBER(5, 0)) AS PARENT_CUSTOMER_ID,
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
        CAST(TRIM(C0CFAX) AS VARCHAR(10)) AS CONTACT_FAX,
        CAST(TRIM(C0CEML) AS VARCHAR(30)) AS CONTACT_EMAIL,
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
            COALESCE(TRIM(C0CFAX), ''),
            COALESCE(TRIM(C0CEML), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM {{ source('bronze_data', 'T_BRZ_CUSTOMER_MASTER_SACUST') }}
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (
            SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }}
        )
    {% endif %}
),

-- Step 2: Deduplication
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect inserts/updates
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

-- Step 3b: Detect deletes
deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

-- Step 4: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(CUSTOMER_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY CUSTOMER_ID ORDER BY ENTRY_TIMESTAMP
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
        oc.CONTACT_FAX,
        oc.CONTACT_EMAIL,
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

-- Step 7: Expire old current rows when updates happen
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
        old.CONTACT_FAX,
        old.CONTACT_EMAIL,
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

-- Step 8: Soft deletes - expire records if source says DELETE
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
        old.CONTACT_FAX,
        old.CONTACT_EMAIL,
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

-- Final Output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows
