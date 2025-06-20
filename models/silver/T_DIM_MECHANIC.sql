{{ config(
    materialized = 'incremental',
    unique_key = ['MECHANIC_SK']
) }}

WITH source_data AS (
    SELECT        
        CAST(TRIM(AOSTORE) AS INTEGER) AS STORE_ID,
        CAST(TRIM(AOMECH) AS INTEGER) AS MECHANIC_ID,
        CAST(TRIM(AONAME) AS VARCHAR(50)) AS MECHANIC_NAME,
        CAST(TRIM(AOMCTP) AS VARCHAR(1)) AS MECHANIC_TYPE,
        CAST(TRIM(AOEMST) AS INTEGER) AS EMPLOYEE_STORE_NUMBER,
        CAST(TRIM(AOEMP) AS INTEGER) AS EMPLOYEE_ID,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_MECHANICS_WOMECH') }}
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY STORE_ID,MECHANIC_ID
        ORDER BY TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) DESC
    ) = 1

),

joined_data_with_checksum AS (
    SELECT 
        sd.*,
        -- Add joined dimension keys
        CAST(dim.STORE_SK AS NUMBER(20,0)) AS STORE_SK,
        -- MD5 hash calculation
        MD5(CONCAT_WS('|',
            COALESCE(sd.MECHANIC_NAME, ''),
            COALESCE(sd.MECHANIC_TYPE, ''),
            COALESCE(CAST(sd.EMPLOYEE_STORE_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(sd.EMPLOYEE_ID AS VARCHAR), ''),
            COALESCE(CAST(dim.STORE_SK AS VARCHAR), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM source_data sd
    LEFT JOIN {{ ref('T_DIM_STORE') }} dim 
        ON dim.STORE_NUMBER = sd.STORE_ID 
        AND sd.ENTRY_TIMESTAMP BETWEEN dim.EFFECTIVE_DATE 
        AND COALESCE(dim.EXPIRATION_DATE, '9999-12-31')
),

-- Step 2: Split for changes and deletes
changes AS (
    SELECT * FROM joined_data_with_checksum
    WHERE OPERATION IN ('INSERT', 'UPDATE')
),
deletes AS (
    SELECT * FROM joined_data_with_checksum
    WHERE OPERATION = 'DELETE'
),

max_key AS (
    SELECT COALESCE(MAX(MECHANIC_SK), 0) AS max_sk FROM {{ this }}
),

new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.STORE_ID, oc.MECHANIC_ID, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS MECHANIC_SK,
        oc.STORE_ID,
        oc.MECHANIC_ID,
        oc.MECHANIC_NAME,
        oc.MECHANIC_TYPE,
        oc.EMPLOYEE_STORE_NUMBER,
        oc.EMPLOYEE_ID,
        CAST(dim.STORE_SK AS number(20,0) ) AS STORE_SK,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        '9999-12-31 23:59:59' AS EXPIRATION_DATE,
        TRUE AS IS_CURRENT_FLAG,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM changes oc
    CROSS JOIN max_key
    LEFT JOIN {{ ref('T_DIM_STORE') }} dim
      ON dim.STORE_NUMBER = oc.STORE_ID
     AND oc.ENTRY_TIMESTAMP BETWEEN dim.EFFECTIVE_DATE AND COALESCE(dim.EXPIRATION_DATE, '9999-12-31')
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.STORE_ID = oc.STORE_ID
          AND tgt.MECHANIC_ID = oc.MECHANIC_ID
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

expired_rows AS (
    SELECT
        old.MECHANIC_SK,
        old.STORE_ID,
        old.MECHANIC_ID,
        old.MECHANIC_NAME,
        old.MECHANIC_TYPE,
        old.EMPLOYEE_STATUS,
        old.EMPLOYEE_ID,
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
      ON old.STORE_ID = new.STORE_ID
     AND old.MECHANIC_ID = new.MECHANIC_ID
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

soft_deleted_rows AS (
    SELECT
        old.MECHANIC_SK,
        old.STORE_ID,
        old.MECHANIC_ID,
        old.MECHANIC_NAME,
        old.MECHANIC_TYPE,
        old.EMPLOYEE_STATUS,
        old.EMPLOYEE_ID,
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
      ON old.STORE_ID = del.STORE_ID
     AND old.MECHANIC_ID = del.MECHANIC_ID
     AND old.IS_CURRENT_FLAG = TRUE
)

SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows