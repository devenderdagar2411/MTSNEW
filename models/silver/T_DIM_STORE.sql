{{ config(
    materialized = 'incremental',
    unique_key = ['STORE_SK']
) }}

WITH source_data AS (
    SELECT
        CAST(TRIM(A4STORE) AS NUMBER(3,0)) AS STORE_NUMBER,
        CAST(TRIM(A4PSTN) AS NUMBER(3,0)) AS PARENT_STORE_NUMBER,
        CAST(TRIM(A4NAME) AS VARCHAR(40)) AS STORE_NAME,
        CAST(TRIM(A4ADR1) AS VARCHAR(30)) AS ADDRESS_LINE_1,
        CAST(TRIM(A4ADR2) AS VARCHAR(30)) AS ADDRESS_LINE_2,
        CAST(TRIM(A4ADR3) AS VARCHAR(30)) AS ADDRESS_LINE_3,
        CAST(TRIM(A4CITY) AS VARCHAR(20)) AS CITY,
        CAST(TRIM(A4STAT) AS VARCHAR(2)) AS STATE,
        CAST(TRIM(A4ZIP) AS VARCHAR(9)) AS ZIP_CODE,
        CAST(TRIM(A4FDID) AS VARCHAR(9)) AS FEDERAL_ID,
        CAST(TRIM(A4STID) AS VARCHAR(9)) AS STATE_ID,
        CAST(TRIM(A4WIPX) AS VARCHAR(3)) AS POS_PREFIX,
        CAST(NULLIF(TRIM(A4LO), '') AS NUMBER(3,0)) AS LOCATION_NUMBER,
        CAST(NULLIF(TRIM(A4CO), '') AS NUMBER(3,0)) AS MASTER_COMPANY_NUMBER,
        CAST(TRIM(A4MSWX) AS VARCHAR(1)) AS STORE_MASTER_WAREHOUSE_FLAG,
        CAST(NULLIF(TRIM(A4MWST), '') AS NUMBER(3,0)) AS MASTER_WAREHOUSE_NUMBER,
        CAST(NULLIF(TRIM(A4TERR), '') AS NUMBER(3,0)) AS SALES_TERRITORY,
        CAST(TRIM(A4PHN1) AS VARCHAR(10)) AS PHONE_NUMBER_1,
        CAST(TRIM(A4PEX1) AS VARCHAR(4)) AS EXTENSION_1,
        CAST(TRIM(A4PHN2) AS VARCHAR(10)) AS PHONE_NUMBER_2,
        CAST(TRIM(A4PEX2) AS VARCHAR(4)) AS EXTENSION_2,
        CAST(TRIM(A4PHN3) AS VARCHAR(10)) AS PHONE_NUMBER_3,
        CAST(TRIM(A4PEX3) AS VARCHAR(4)) AS EXTENSION_3,
        CAST(TRIM(A4FAX) AS VARCHAR(10)) AS FAX_NUMBER,
        CAST(TRIM(A4PRT) AS VARCHAR(10)) AS PRINTER,
        CAST(NULLIF(TRIM(A4TDCD), '') AS NUMBER(10,0)) AS TAX_DISTRICT_CODE,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
           COALESCE(TRIM(A4PSTN), ''),
            COALESCE(TRIM(A4NAME), ''),
            COALESCE(TRIM(A4ADR1), ''),
            COALESCE(TRIM(A4ADR2), ''),
            COALESCE(TRIM(A4ADR3), ''),
            COALESCE(TRIM(A4CITY), ''),
            COALESCE(TRIM(A4STAT), ''),
            COALESCE(TRIM(A4ZIP), ''),
            COALESCE(TRIM(A4FDID), ''),
            COALESCE(TRIM(A4STID), ''),
            COALESCE(TRIM(A4WIPX), ''),
            COALESCE(TRIM(A4LO), ''),
            COALESCE(TRIM(A4CO), ''),
            COALESCE(TRIM(A4MSWX), ''),
            COALESCE(TRIM(A4MWST), ''),
            COALESCE(TRIM(A4TERR), ''),
            COALESCE(TRIM(A4PHN1), ''),
            COALESCE(TRIM(A4PEX1), ''),
            COALESCE(TRIM(A4PHN2), ''),
            COALESCE(TRIM(A4PEX2), ''),
            COALESCE(TRIM(A4PHN3), ''),
            COALESCE(TRIM(A4PEX3), ''),
            COALESCE(TRIM(A4FAX), ''),
            COALESCE(TRIM(A4PRT), ''),
            COALESCE(TRIM(A4TDCD), '')
        )) AS RECORD_CHECKSUM_HASH,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_STORE_MASTER_STMAST') }}
    {% if is_incremental() %}
    WHERE ENTRY_TIMESTAMP ='1900-01-01T00:00:00Z'
        --WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }})
    {% endif %}
),

ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY STORE_NUMBER
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
            PARTITION BY STORE_NUMBER ORDER BY ENTRY_TIMESTAMP
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

max_key AS (
    SELECT COALESCE(MAX(STORE_SK), 0) AS max_sk FROM {{ this }}
),

ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY STORE_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.STORE_NUMBER, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS STORE_SK,
        oc.STORE_NUMBER,
        oc.PARENT_STORE_NUMBER,
        oc.STORE_NAME,
        oc.ADDRESS_LINE_1,
        oc.ADDRESS_LINE_2,
        oc.ADDRESS_LINE_3,
        oc.CITY,
        oc.STATE,
        oc.ZIP_CODE,
        oc.FEDERAL_ID,
        oc.STATE_ID,
        oc.POS_PREFIX,
        oc.LOCATION_NUMBER,
        oc.MASTER_COMPANY_NUMBER,
        oc.STORE_MASTER_WAREHOUSE_FLAG,
        oc.MASTER_WAREHOUSE_NUMBER,
        oc.SALES_TERRITORY,
        oc.PHONE_NUMBER_1,
        oc.EXTENSION_1,
        oc.PHONE_NUMBER_2,
        oc.EXTENSION_2,
        oc.PHONE_NUMBER_3,
        oc.EXTENSION_3,
        oc.FAX_NUMBER,
        oc.PRINTER,
        oc.TAX_DISTRICT_CODE,
        NULL AS REGION,
        NULL AS COUNTY,
        NULL AS COUNTRY,
        NULL AS LATITUDE,
        NULL AS LONGITUDE,
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
        WHERE tgt.STORE_NUMBER = oc.STORE_NUMBER
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

expired_rows AS (
    SELECT
        old.STORE_SK,
        old.STORE_NUMBER,
        old.PARENT_STORE_NUMBER,
        old.STORE_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.FEDERAL_ID,
        old.STATE_ID,
        old.POS_PREFIX,
        old.LOCATION_NUMBER,
        old.MASTER_COMPANY_NUMBER,
        old.STORE_MASTER_WAREHOUSE_FLAG,
        old.MASTER_WAREHOUSE_NUMBER,
        old.SALES_TERRITORY,
        old.PHONE_NUMBER_1,
        old.EXTENSION_1,
        old.PHONE_NUMBER_2,
        old.EXTENSION_2,
        old.PHONE_NUMBER_3,
        old.EXTENSION_3,
        old.FAX_NUMBER,
        old.PRINTER,
        old.TAX_DISTRICT_CODE,
        old.REGION,
        old.COUNTY,
        old.COUNTRY,
        old.LATITUDE,
        old.LONGITUDE,
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

soft_deletes AS (
    SELECT
        old.STORE_SK,
        old.STORE_NUMBER,
        old.PARENT_STORE_NUMBER,
        old.STORE_NAME,
        old.ADDRESS_LINE_1,
        old.ADDRESS_LINE_2,
        old.ADDRESS_LINE_3,
        old.CITY,
        old.STATE,
        old.ZIP_CODE,
        old.FEDERAL_ID,
        old.STATE_ID,
        old.POS_PREFIX,
        old.LOCATION_NUMBER,
        old.MASTER_COMPANY_NUMBER,
        old.STORE_MASTER_WAREHOUSE_FLAG,
        old.MASTER_WAREHOUSE_NUMBER,
        old.SALES_TERRITORY,
        old.PHONE_NUMBER_1,
        old.EXTENSION_1,
        old.PHONE_NUMBER_2,
        old.EXTENSION_2,
        old.PHONE_NUMBER_3,
        old.EXTENSION_3,
        old.FAX_NUMBER,
        old.PRINTER,
        old.TAX_DISTRICT_CODE,
        old.REGION,
        old.COUNTY,
        old.COUNTRY,
        old.LATITUDE,
        old.LONGITUDE,
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
