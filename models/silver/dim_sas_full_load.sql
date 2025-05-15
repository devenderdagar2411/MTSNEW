{{
  config(
    materialized = 'incremental',
    unique_key = 'SURROGATE_KEY',
    incremental_strategy = 'merge'
  )
}}

WITH source_data AS (
    SELECT        
        M0SLRP,
        M0NAME,
        M0SPCM,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM as RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_Date,
        INGESTION_TIMESTAMP,
        ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(M0SLRP::VARCHAR, ''),
            COALESCE(M0NAME::VARCHAR, ''),
            COALESCE(M0SPCM::VARCHAR, '')
        )) AS TRACKING_HASH
    FROM {{ source('raw_data', 'T_BRZ_SALESREP_MASTER_SASLR_CLONE') }}
),

ranked_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY M0SLRP ORDER BY ENTRY_TIMESTAMP) AS CHANGE_RANK
    FROM source_data
),

current_dim AS (
    SELECT 
        SURROGATE_KEY,
        M0SLRP,
        VALID_FROM,
        VALID_TO,
        IS_CURRENT,
        TRACKING_HASH
    FROM {{ this }}
),

max_key AS (
    SELECT COALESCE(MAX(SURROGATE_KEY), 0) AS MAX_KEY FROM {{ this }}
),

records_to_expire AS (
    SELECT
        cd.SURROGATE_KEY,
        cd.M0SLRP,
        NULL AS M0NAME,
        NULL AS M0SPCM,
        NULL AS SOURCE_SYSTEM,
        NULL AS SOURCE_FILE_NAME,
        NULL AS BATCH_ID,
        NULL AS RECORD_CHECKSUM_HASH,
        NULL AS ETL_VERSION,
        NULL AS INGESTION_DATE,
        NULL AS INGESTION_TIMESTAMP,
        cd.TRACKING_HASH,
        cd.VALID_FROM,
        rs.ENTRY_TIMESTAMP AS VALID_TO,
        FALSE AS IS_CURRENT,
        CURRENT_TIMESTAMP AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY,
        NULL AS CHANGE_RANK,
        cd.SURROGATE_KEY AS SURROGATE_KEY
    FROM current_dim cd
    JOIN ranked_source rs
      ON cd.M0SLRP = rs.M0SLRP
    WHERE rs.ENTRY_TIMESTAMP > cd.VALID_FROM
      AND cd.IS_CURRENT = TRUE
      AND rs.TRACKING_HASH != cd.TRACKING_HASH
),

new_records AS (
    SELECT
        mk.MAX_KEY + ROW_NUMBER() OVER (ORDER BY rs.M0SLRP, rs.ENTRY_TIMESTAMP) AS SURROGATE_KEY,
        rs.M0SLRP,
        rs.M0NAME,
        rs.M0SPCM,
        rs.SOURCE_SYSTEM,
        rs.SOURCE_FILE_NAME,
        rs.BATCH_ID,
        rs.RECORD_CHECKSUM_HASH,
        rs.ETL_VERSION,
        rs.INGESTION_DATE,
        rs.INGESTION_TIMESTAMP,
        rs.TRACKING_HASH,
        rs.ENTRY_TIMESTAMP AS VALID_FROM,
        NULL AS VALID_TO,
        TRUE AS IS_CURRENT,
        CURRENT_TIMESTAMP AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY,
        rs.CHANGE_RANK
    FROM ranked_source rs
    LEFT JOIN current_dim cd
      ON rs.M0SLRP = cd.M0SLRP AND cd.IS_CURRENT = TRUE
    CROSS JOIN max_key mk
    WHERE cd.M0SLRP IS NULL 
       OR (rs.ENTRY_TIMESTAMP > cd.VALID_FROM AND rs.TRACKING_HASH != cd.TRACKING_HASH)
)

SELECT * FROM records_to_expire
UNION ALL
SELECT * FROM new_records
