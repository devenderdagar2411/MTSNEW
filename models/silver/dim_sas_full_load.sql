{{  
  config(
    materialized = 'incremental',
    unique_key = ['M0SLRP', 'VALID_FROM'],
    incremental_strategy = 'merge'
  )
}}

-- Extract source and calculate tracking hash
WITH source_data AS (
    SELECT        
        M0SLRP,
        M0NAME,
        M0SPCM,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM,
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

-- Rank source changes by M0SLRP and ENTRY_TIMESTAMP
ranked_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY M0SLRP ORDER BY ENTRY_TIMESTAMP) AS CHANGE_RANK
    FROM source_data
)

{% if is_incremental() %}

-- Get current dimension table state
, current_dim AS (
    SELECT 
    SURROGATE_KEY,
        M0SLRP,
    M0NAME,
    M0SPCM,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    RECORD_CHECKSUM,
    ETL_VERSION,
    INGESTION_Date,
    INGESTION_TIMESTAMP,
    TRACKING_HASH,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT,
    DBT_UPDATED_AT,
    DBT_UPDATED_BY
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
),

-- Get max surrogate key for generating new ones
max_key AS (
    SELECT COALESCE(MAX(SURROGATE_KEY), 0) AS MAX_KEY FROM {{ this }}
),

-- Expire old records with changed data
records_to_expire AS (
    SELECT
		cd.SURROGATE_KEY,
        cd.M0SLRP,
        cd.M0NAME,
        cd.M0SPCM,
        cd.SOURCE_SYSTEM,
        cd.SOURCE_FILE_NAME,
        cd.BATCH_ID,
        cd.RECORD_CHECKSUM,
        cd.ETL_VERSION,
        cd.INGESTION_Date,
        cd.INGESTION_TIMESTAMP,
        cd.TRACKING_HASH,
        cd.VALID_FROM,
        rs.ENTRY_TIMESTAMP - INTERVAL '1 second' AS VALID_TO,
        FALSE AS IS_CURRENT,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY
    FROM ranked_source rs
    JOIN current_dim cd
      ON rs.M0SLRP = cd.M0SLRP
    WHERE rs.ENTRY_TIMESTAMP > cd.VALID_FROM
      AND rs.TRACKING_HASH != cd.TRACKING_HASH
),

-- Insert new records (changed or new M0SLRP)
new_records AS (
    SELECT
        mk.MAX_KEY + ROW_NUMBER() OVER (ORDER BY rs.M0SLRP, rs.ENTRY_TIMESTAMP) AS SURROGATE_KEY,
        rs.M0SLRP,
        rs.M0NAME,
        rs.M0SPCM,
        rs.SOURCE_SYSTEM,
        rs.SOURCE_FILE_NAME,
        rs.BATCH_ID,
        rs.RECORD_CHECKSUM,
        rs.ETL_VERSION,
        rs.INGESTION_Date,
        rs.INGESTION_TIMESTAMP,
        rs.TRACKING_HASH,
        rs.ENTRY_TIMESTAMP AS VALID_FROM,
        NULL AS VALID_TO,
        TRUE AS IS_CURRENT,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY
    FROM ranked_source rs
    LEFT JOIN current_dim cd
      ON rs.M0SLRP = cd.M0SLRP
    CROSS JOIN max_key mk
    WHERE cd.M0SLRP IS NULL 
       OR (rs.ENTRY_TIMESTAMP > cd.VALID_FROM AND rs.TRACKING_HASH != cd.TRACKING_HASH)
)

-- Final output: expired and new records
SELECT * FROM records_to_expire
UNION ALL
SELECT * FROM new_records

{% else %}

-- Initial full load: keep only latest record per M0SLRP
SELECT 
    ROW_NUMBER() OVER (ORDER BY M0SLRP, ENTRY_TIMESTAMP) AS SURROGATE_KEY,
    M0SLRP,
    M0NAME,
    M0SPCM,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    RECORD_CHECKSUM,
    ETL_VERSION,
    INGESTION_Date,
    INGESTION_TIMESTAMP,
    TRACKING_HASH,
    ENTRY_TIMESTAMP AS VALID_FROM,
    NULL AS VALID_TO,
    TRUE AS IS_CURRENT,
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT,
    'DBT' AS DBT_UPDATED_BY
FROM ranked_source
QUALIFY ROW_NUMBER() OVER (PARTITION BY M0SLRP ORDER BY ENTRY_TIMESTAMP DESC) = 1

{% endif %}
