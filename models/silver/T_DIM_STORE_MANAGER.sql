{{ config(
    materialized = 'incremental',
    unique_key = ['STORE_MANAGER_SK']
) }}

WITH source_data AS (
    SELECT
        CAST(NULLIF(TRIM(src.M3STORE), '') AS NUMBER(5, 0)) AS STORE_NUMBER,
        CAST(TRIM(src.M3NAME) AS VARCHAR(40)) AS MANAGER_NAME,
        CAST(TRIM(src."M3CELL#") AS VARCHAR(10)) AS CELL_NUMBER,
        CAST(TRIM(src.M3EMAIL) AS VARCHAR(60)) AS EMAIL_ADDRESS,
        CAST(NULLIF(TRIM(src.M3SLRP), '') AS NUMBER(5, 0)) AS BASYS_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(src.M3RSM), '') AS NUMBER(5, 0)) AS REGIONAL_SALES_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(src.M3ROM), '') AS NUMBER(5, 0)) AS REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(src.M3ADM), '') AS NUMBER(5, 0)) AS AREA_DIRECTOR_MANAGER_NUMBER,
        CAST(NULLIF(TRIM(src.M3PAY), '') AS NUMBER(5, 0)) AS PAYROLL_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(src.M3MGR), '') AS NUMBER(5, 0)) AS STORE_MANAGER_SALESREP_NUMBER,
        CAST(NULLIF(TRIM(src.M3MFGROM), '') AS NUMBER(5, 0)) AS MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER,
        CAST(TRIM(src.SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(src.SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(src.BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(src.ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(src.OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(src.ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_STOREMANAGER_STMGRS') }} src
    {% if is_incremental() %}
    WHERE ENTRY_TIMESTAMP > (SELECT MAX(EFFECTIVE_DATE) FROM {{ this }})
	{% else %}
        select '1900-01-01T00:00:00Z' as max_loaded_ts
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(NULLIF(TRIM(src.M3STORE), '') AS NUMBER(5, 0))
        ORDER BY TO_TIMESTAMP_NTZ(TRIM(src.ENTRY_TIMESTAMP)) DESC
    ) = 1
),

-- Step 2: Join with dimension and calculate hash
source_with_hash AS (
    SELECT
        src.*,
        dim.STORE_SK,
        MD5(CONCAT_WS('|',
            COALESCE(src.MANAGER_NAME, ''),
            COALESCE(src.CELL_NUMBER, ''),
            COALESCE(src.EMAIL_ADDRESS, ''),
            COALESCE(CAST(src.BASYS_SALESREP_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.REGIONAL_SALES_MANAGER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.REGIONAL_OPERATIONS_MANAGER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.AREA_DIRECTOR_MANAGER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.PAYROLL_SALESREP_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.STORE_MANAGER_SALESREP_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(dim.STORE_SK AS VARCHAR), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM source_data src
    LEFT JOIN {{ ref('T_DIM_STORE') }} dim
      ON dim.STORE_NUMBER = src.STORE_NUMBER
     AND src.ENTRY_TIMESTAMP BETWEEN dim.EFFECTIVE_DATE AND COALESCE(dim.EXPIRATION_DATE, '9999-12-31')
),

-- Step 3: Identify changes (inserts/updates)
changes AS (
    SELECT *
    FROM source_with_hash
    WHERE OPERATION IN ('INSERT', 'UPDATE')
),

-- Step 4: Detect deletes
deletes AS (
    SELECT *
    FROM source_with_hash
    WHERE OPERATION = 'DELETE'
),

-- Step 5: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(STORE_MANAGER_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 6: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY STORE_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 7: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.STORE_NUMBER, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS STORE_MANAGER_SK,
        oc.STORE_NUMBER,
        oc.MANAGER_NAME,
        oc.CELL_NUMBER,
        oc.EMAIL_ADDRESS,
        oc.BASYS_SALESREP_NUMBER,
        oc.REGIONAL_SALES_MANAGER_NUMBER,
        oc.REGIONAL_OPERATIONS_MANAGER_NUMBER,
        oc.AREA_DIRECTOR_MANAGER_NUMBER,
        oc.PAYROLL_SALESREP_NUMBER,
        oc.STORE_MANAGER_SALESREP_NUMBER,
        oc.MFG_REGIONAL_OPERATIONS_MANAGER_NUMBER,
        oc.STORE_SK,
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
        WHERE tgt.STORE_NUMBER = oc.STORE_NUMBER
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 8: Expire old current rows when updates happen
expired_rows AS (
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
      ON old.STORE_NUMBER = new.STORE_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 9: Soft deletes - expire records if source says DELETE
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

-- Final output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deletes
UNION ALL
SELECT * FROM new_rows