{{ config(
    materialized='incremental',
    unique_key='M0SLRP',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        M0SLRP,
        M0NAME,
        M0SPCM,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,
        md5(
            coalesce(M0SLRP, '') || '|' ||
            coalesce(M0NAME, '') || '|' ||
            coalesce(M0SPCM, '') || '|' ||, '')
        ) AS row_hash,
        ENTRY_TIMESTAMP AS EFFECTIVE_START_DATE,
        NULL AS EFFECTIVE_END_DATE,
        TRUE AS CURRENT_FLAG
    FROM {{ source('raw_data', 'T_BRZ_SALESREP_MASTER_SASLRM') }}
),

-- Get new/changed records not already in target
new_records AS (
    SELECT s.*
    FROM source_data s
    LEFT JOIN {{ this }} t
      ON s.M0SLRP = t.M0SLRP
     AND s.row_hash = t.row_hash
     AND t.CURRENT_FLAG = TRUE
    WHERE t.M0SLRP IS NULL
)

SELECT * FROM new_records
