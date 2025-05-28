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
        CAST(TRIM(AOUSER) AS VARCHAR(10)) AS LAST_MODIFIED_USER,
        TO_DATE(CAST(TRIM(AOCYMD) AS VARCHAR), 'YYYYMMDD') AS LAST_MODIFIED_DATE,
        CAST(TRIM(AOHMS) AS INTEGER) AS LAST_MODIFIED_TIME,
        CAST(TRIM(AOWKSN) AS VARCHAR(10)) AS WORKSTATION_ID,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(AONAME), ''),
            COALESCE(TRIM(AOMCTP), ''),
            COALESCE(TRIM(AOEMST), ''),
            COALESCE(TRIM(AOEMP), ''),
            COALESCE(TRIM(AOUSER), ''),
            COALESCE(TRIM(AOCYMD), ''),
            COALESCE(TRIM(AOHMS), ''),
            COALESCE(TRIM(AOWKSN), '')
        )) AS RECORD_CHECKSUM_HASH,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_MECHANICS_WOMECH') }}
    {% if is_incremental() %}
        WHERE TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) > (
            SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }}
        )
    {% endif %}
),

ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY STORE_ID, MECHANIC_ID
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),

deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

source_with_lag AS (
    SELECT
        curr.*,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY STORE_ID, MECHANIC_ID ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),

changes AS (
    SELECT *
    FROM source_with_lag
    WHERE (RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL)
      AND OPERATION != 'DELETE'
),

deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

{% if is_incremental() %}
max_key AS (
    SELECT COALESCE(MAX(MECHANIC_SK), 0) AS max_sk FROM {{ this }}
),
{% else %}
max_key AS (
    SELECT 0 AS max_sk
),
{% endif %}

ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY STORE_ID, MECHANIC_ID ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
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
        CAST(NULL AS BIGINT) AS STORE_SK,
        oc.LAST_MODIFIED_USER,
        oc.LAST_MODIFIED_DATE,
        oc.LAST_MODIFIED_TIME,
        oc.WORKSTATION_ID,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')
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
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.STORE_ID = oc.STORE_ID
          AND tgt.MECHANIC_ID = oc.MECHANIC_ID
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
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
        old.LAST_MODIFIED_USER,
        old.LAST_MODIFIED_DATE,
        old.LAST_MODIFIED_TIME,
        old.WORKSTATION_ID,
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
        old.LAST_MODIFIED_USER,
        old.LAST_MODIFIED_DATE,
        old.LAST_MODIFIED_TIME,
        old.WORKSTATION_ID,
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