{{ config(
    materialized = 'incremental',
    unique_key = ['SALES_REP_SK']
) }}

-- Step 1: Load new data from source
WITH source_data AS (
    SELECT        
        CAST(TRIM(M0SLRP) AS NUMBER(5, 0)) AS SALES_REP_NUMBER,
        CAST(TRIM(M0NAME) AS VARCHAR(50)) AS SALES_REP_NAME,
        CAST(TRIM(M0SPCM) AS VARCHAR(1)) AS SPIFF_COMMISSION_FLAG,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(M0NAME), ''),
            COALESCE(TRIM(M0SPCM), '')
        )) AS RECORD_CHECKSUM_HASH,        
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_SALESREP_MASTER_SASLRM') }}
    {% if is_incremental() %}
    WHERE ENTRY_TIMESTAMP ='1900-01-01T00:00:00Z'
        --WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Step 2: Deduplication
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY SALES_REP_NUMBER
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
            PARTITION BY SALES_REP_NUMBER ORDER BY ENTRY_TIMESTAMP
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
    SELECT COALESCE(MAX(SALES_REP_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY SALES_REP_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.SALES_REP_NUMBER, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS SALES_REP_SK,
        oc.SALES_REP_NUMBER,
        oc.SALES_REP_NAME,
        oc.SPIFF_COMMISSION_FLAG,
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
        WHERE tgt.SALES_REP_NUMBER = oc.SALES_REP_NUMBER
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old current rows when updates happen
expired_rows AS (
    SELECT
        old.SALES_REP_SK,
        old.SALES_REP_NUMBER,
        old.SALES_REP_NAME,
        old.SPIFF_COMMISSION_FLAG,
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
      ON old.SALES_REP_NUMBER = new.SALES_REP_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes - expire records if source says DELETE
soft_deleted_rows AS (
    SELECT
        old.SALES_REP_SK,
        old.SALES_REP_NUMBER,
        old.SALES_REP_NAME,
        old.SPIFF_COMMISSION_FLAG,
        old.EFFECTIVE_DATE,
        del.ENTRY_TIMESTAMP  AS EXPIRATION_DATE,
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
      ON old.SALES_REP_NUMBER = del.SALES_REP_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final Output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows
