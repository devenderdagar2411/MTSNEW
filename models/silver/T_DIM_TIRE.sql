{{ config(
    materialized = 'incremental',
    unique_key = ['TIRE_VENDOR_SK']
) }}

-- Step 1: Load and deduplicate source data using QUALIFY
WITH source_data AS (
    SELECT
        CAST(TRIM(K3CCCD) AS NUMBER(3,0)) AS VENDOR_TIER_CODE,
        CAST(TRIM(K3NAME) AS VARCHAR(40)) AS VENDOR_NAME,
        CAST(TRIM(K3CB) AS VARCHAR(1)) AS COST_BLOCK_FLAG,
        CAST(TRIM(K3POSCA) AS VARCHAR(1)) AS POSCA,
        CAST(TRIM(K3MINGP) AS NUMBER(8,6)) AS MINIMUM_GROSS_PROFIT_PERCENT,
        CAST(TRIM(K3EX001) AS VARCHAR(1)) AS EXTENSION_FIELD_1,
        CAST(TRIM(K3EX002) AS VARCHAR(1)) AS EXTENSION_FIELD_2,
        CAST(TRIM(K3EX003) AS VARCHAR(1)) AS EXTENSION_FIELD_3,
        CAST(TRIM(K3EX004) AS VARCHAR(1)) AS EXTENSION_FIELD_4,
        CAST(TRIM(K3EX005) AS VARCHAR(1)) AS EXTENSION_FIELD_5,
        CAST(TRIM(K3EX016) AS VARCHAR(20)) AS EXTENSION_FIELD_16,
        CAST(TRIM(K3EX017) AS VARCHAR(20)) AS EXTENSION_FIELD_17,
        CAST(TRIM(K3EX018) AS VARCHAR(20)) AS EXTENSION_FIELD_18,
        CAST(TRIM(K3EX019) AS VARCHAR(20)) AS EXTENSION_FIELD_19,
        CAST(TRIM(K3EX026) AS NUMBER(15,6)) AS EXTENSION_FIELD_26,
        CAST(TRIM(K3EX027) AS NUMBER(15,6)) AS EXTENSION_FIELD_27,
        CAST(TRIM(K3EX028) AS NUMBER(15,6)) AS EXTENSION_FIELD_28,
        CAST(TRIM(K3EX029) AS NUMBER(15,6)) AS EXTENSION_FIELD_29,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(K3NAME), ''),
            COALESCE(TRIM(K3CB), ''),
            COALESCE(TRIM(K3POSCA), ''),
            COALESCE(TRIM(K3MINGP), ''),
            COALESCE(TRIM(K3EX001), ''),
            COALESCE(TRIM(K3EX002), ''),
            COALESCE(TRIM(K3EX003), ''),
            COALESCE(TRIM(K3EX004), ''),
            COALESCE(TRIM(K3EX005), ''),
            COALESCE(TRIM(K3EX016), ''),
            COALESCE(TRIM(K3EX017), ''),
            COALESCE(TRIM(K3EX018), ''),
            COALESCE(TRIM(K3EX019), ''),
            COALESCE(TRIM(K3EX026), ''),
            COALESCE(TRIM(K3EX027), ''),
            COALESCE(TRIM(K3EX028), ''),
            COALESCE(TRIM(K3EX029), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM {{ source('bronze_data', 'T_BRZ_TIRE_MASTER_ITCLVT') }}
    {% if is_incremental() %}
    WHERE TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) > (
        SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }}
    )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(TRIM(K3CCCD) AS NUMBER(3,0))
        ORDER BY TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) DESC
    ) = 1
),

-- Step 2: Split for changes and deletes
changes AS (
    SELECT * FROM source_data
    WHERE OPERATION IN ('INSERT', 'UPDATE')
),
deletes AS (
    SELECT * FROM source_data
    WHERE OPERATION = 'DELETE'
),

-- Step 3: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(TIRE_VENDOR_SK), 0) AS max_sk FROM {{ this }}
),
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.VENDOR_TIER_CODE, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS TIRE_VENDOR_SK,
        oc.VENDOR_TIER_CODE,
        oc.VENDOR_NAME,
        oc.COST_BLOCK_FLAG,
        oc.POSCA,
        oc.MINIMUM_GROSS_PROFIT_PERCENT,
        oc.EXTENSION_FIELD_1,
        oc.EXTENSION_FIELD_2,
        oc.EXTENSION_FIELD_3,
        oc.EXTENSION_FIELD_4,
        oc.EXTENSION_FIELD_5,
        oc.EXTENSION_FIELD_16,
        oc.EXTENSION_FIELD_17,
        oc.EXTENSION_FIELD_18,
        oc.EXTENSION_FIELD_19,
        oc.EXTENSION_FIELD_26,
        oc.EXTENSION_FIELD_27,
        oc.EXTENSION_FIELD_28,
        oc.EXTENSION_FIELD_29,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS EXPIRATION_DATE,
        TRUE AS IS_CURRENT_FLAG,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        CURRENT_TIMESTAMP AS INGESTION_DTTM,
        CURRENT_DATE AS INGESTION_DT
    FROM changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.VENDOR_TIER_CODE = oc.VENDOR_TIER_CODE
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

expired_rows AS (
    SELECT
        old.TIRE_VENDOR_SK,
        old.VENDOR_TIER_CODE,
        old.VENDOR_NAME,
        old.COST_BLOCK_FLAG,
        old.POSCA,
        old.MINIMUM_GROSS_PROFIT_PERCENT,
        old.EXTENSION_FIELD_1,
        old.EXTENSION_FIELD_2,
        old.EXTENSION_FIELD_3,
        old.EXTENSION_FIELD_4,
        old.EXTENSION_FIELD_5,
        old.EXTENSION_FIELD_16,
        old.EXTENSION_FIELD_17,
        old.EXTENSION_FIELD_18,
        old.EXTENSION_FIELD_19,
        old.EXTENSION_FIELD_26,
        old.EXTENSION_FIELD_27,
        old.EXTENSION_FIELD_28,
        old.EXTENSION_FIELD_29,
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
      ON old.VENDOR_TIER_CODE = new.VENDOR_TIER_CODE
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

soft_deletes AS (
    SELECT
        old.TIRE_VENDOR_SK,
        old.VENDOR_TIER_CODE,
        old.VENDOR_NAME,
        old.COST_BLOCK_FLAG,
        old.POSCA,
        old.MINIMUM_GROSS_PROFIT_PERCENT,
        old.EXTENSION_FIELD_1,
        old.EXTENSION_FIELD_2,
        old.EXTENSION_FIELD_3,
        old.EXTENSION_FIELD_4,
        old.EXTENSION_FIELD_5,
        old.EXTENSION_FIELD_16,
        old.EXTENSION_FIELD_17,
        old.EXTENSION_FIELD_18,
        old.EXTENSION_FIELD_19,
        old.EXTENSION_FIELD_26,
        old.EXTENSION_FIELD_27,
        old.EXTENSION_FIELD_28,
        old.EXTENSION_FIELD_29,
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
      ON old.VENDOR_TIER_CODE = del.VENDOR_TIER_CODE
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deletes
UNION ALL
SELECT * FROM new_rows