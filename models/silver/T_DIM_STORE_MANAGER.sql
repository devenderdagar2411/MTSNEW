{{ config(
    materialized = 'incremental',
    unique_key = ['STORE_MANAGER_SK']
) }}

-- Step 1: Load new data from source
WITH source_data AS (
    select CAST(TRIM(M3STORE) AS NUMBER(3, 0)) AS STORE_NUMBER,
    SELECT
        CAST(TRIM(M3STORE) AS NUMBER(3, 0)) AS STORE_NUMBER,
        CAST(TRIM(M3NAME) AS VARCHAR(40)) AS MANAGER_NAME,
        CAST(TRIM("M3CELL#") AS VARCHAR(10)) AS CELL_NUMBER,
        CAST(TRIM(M3EMAIL) AS VARCHAR(60)) AS EMAIL_ADDRESS,
        CAST(TRIM(M3SLRP) AS NUMBER(5, 0)) AS BASYS_SALESREP_NUMBER,
        CAST(TRIM(M3RSM) AS NUMBER(5, 0)) AS REGIONAL_SALES_MANAGER_NUMBER,
        CAST(TRIM(M3ROM) AS NUMBER(5, 0)) AS REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(TRIM(M3ADM) AS NUMBER(5, 0)) AS AREA_DIRECTOR_MANAGER_NUMBER,
        CAST(TRIM(M3PAY) AS NUMBER(5, 0)) AS PAYROLL_SALESREP_NUMBER,
        CAST(TRIM(M3MGR) AS NUMBER(5, 0)) AS STORE_MANAGER_SALESREP_NUMBER,
        CAST(TRIM(M3MFGROM) AS NUMBER(5, 0)) AS MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(M3SLRP), '') AS NUMBER(5, 0)) AS BASYS_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(M3RSM), '') AS NUMBER(5, 0)) AS REGIONAL_SALES_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(M3ROM), '') AS NUMBER(5, 0)) AS REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(M3ADM), '') AS NUMBER(5, 0)) AS AREA_DIRECTOR_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(M3PAY), '') AS NUMBER(5, 0)) AS PAYROLL_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(M3MGR), '') AS NUMBER(5, 0)) AS STORE_MANAGER_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(M3MFGROM), '') AS NUMBER(5, 0)) AS MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,

        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(M3NAME), ''),
            COALESCE(TRIM("M3CELL#"), ''),
            COALESCE(TRIM(M3MGR), ''),
            COALESCE(TRIM(M3MFGROM), '')
        )) AS RECORD_CHECKSUM_HASH,

        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP

    FROM {{ source('bronze_data', 'T_BRZ_STOREMANAGER_STMGRS') }}

    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (
            SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }}
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect changes
-- Step 3: Detect inserts/updates
source_with_lag AS (
    SELECT
        curr.*,
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL
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
    SELECT COALESCE(MAX(STORE_MANAGER_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates
-- Step 5: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
    FROM changes
),

-- Step 6: Generate new rows
-- Step 6: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
    CROSS JOIN max_key
    LEFT JOIN {{ ref('T_DIM_STORE') }} dim
      ON dim.STORE_NUMBER = oc.STORE_NUMBER
     AND oc.ENTRY_TIMESTAMP BETWEEN dim.EFFECTIVE_DATE AND COALESCE(dim.EXPIRATION_DATE, '9999-12-31')
     AND dim.IS_CURRENT_FLAG = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} tgt
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.STORE_NUMBER = oc.STORE_NUMBER
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old current rows
-- Step 7: Expire old current rows when updates happen
expired_rows AS (
    SELECT
        old.STORE_MANAGER_SK,
      ON old.STORE_NUMBER = new.STORE_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes - expire records if source says DELETE
soft_deletes AS (
    SELECT
        old.STORE_MANAGER_SK,
        old.STORE_NUMBER,
        old.MANAGER_NAME,
        old.CELL_NUMBER,
        old.EMAIL_ADDRESS,
        old.BASYS_SALESREP_NUMBER,
        old.REGIONAL_SALES_MANAGER_NUMBER,
        old.REGIONAL_OPERATIONS_MANAGER_NUMBER,
        old.AREA_DIRECTOR_MANAGER_NUMBER,
        old.PAYROLL_SALESREP_NUMBER,
        old.STORE_MANAGER_SALESREP_NUMBER,
        old.MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER,
        old.STORE_SK,
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
      ON old.STORE_NUMBER = del.STORE_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Step 8: Final Output
-- Final output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deletes
UNION ALL
SELECT * FROM new_rows