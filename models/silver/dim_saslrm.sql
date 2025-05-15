{{
  config(
    materialized = 'incremental',
    unique_key = 'SURROGATE_KEY',
    incremental_strategy = 'delete+insert',
    alias = 'dim_saslrm'
  )
}}

WITH source_data AS (
    SELECT
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        M0SLRP as BUSINESS_KEY ,
        M0NAME,
        M0SPCM,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT,
        -- Create hash to track changes specifically in M0NAME and M0SPCM using Snowflake syntax
        MD5(CONCAT(
            COALESCE(M0SLRP::VARCHAR, ''), '|',
            COALESCE(M0NAME::VARCHAR, ''), '|',
            COALESCE(M0SPCM::VARCHAR, '')
        )) as NAME_SPCM_HASH
     FROM {{ source('raw_data', 'T_BRZ_SALESREP_MASTER_SASLRM') }}
),

-- Rank changes within the 24-hour period to capture all changes to M0SLRP
ranked_changes AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY BUSINESS_KEY 
            ORDER BY ENTRY_TIMESTAMP
        ) as CHANGE_RANK
    FROM source_data
),

-- Generate business keys for the records
source_with_keys AS (
    SELECT
        BUSINESS_KEY,
        *
    FROM ranked_changes
),

{% if is_incremental() %}
-- Get the current state of the SCD table for comparison
current_scd_data AS (
    SELECT
        M0SLRP as BUSINESS_KEY,
        SURROGATE_KEY,
        VALID_FROM,
        VALID_TO,
        IS_CURRENT,
        NAME_SPCM_HASH,
        -- Get the maximum surrogate key to ensure new keys are incremental
        MAX(SURROGATE_KEY) OVER () as max_SURROGATE_KEY
    FROM {{ this }}
),

-- Get the maximum surrogate key to use for incrementing
max_key AS (
    SELECT MAX(SURROGATE_KEY) as max_SURROGATE_KEY
    FROM current_scd_data
),

-- Identify records that need to be expired (existing records that changed)
-- Now specifically tracking changes in the hashed columns
records_to_expire AS (
    SELECT
        curr.SURROGATE_KEY,
        curr.BUSINESS_KEY,
        curr.VALID_FROM,
        src.ENTRY_TIMESTAMP as VALID_TO,
        FALSE as IS_CURRENT
    FROM current_scd_data curr
    INNER JOIN source_with_keys src
        ON curr.BUSINESS_KEY = src.BUSINESS_KEY
    WHERE (src.ENTRY_TIMESTAMP > curr.VALID_FROM
           AND curr.IS_CURRENT = TRUE
           AND src.NAME_SPCM_HASH != curr.NAME_SPCM_HASH)
),

-- Assign incremental surrogate keys to new records
new_records AS (
    SELECT
        src.*,
        COALESCE((SELECT max_SURROGATE_KEY FROM max_key), 0) + 
            ROW_NUMBER() OVER (ORDER BY src.BUSINESS_KEY, src.ENTRY_TIMESTAMP) as new_SURROGATE_KEY
    FROM source_with_keys src
    LEFT JOIN current_scd_data curr
        ON src.BUSINESS_KEY = curr.BUSINESS_KEY
        AND src.ENTRY_TIMESTAMP <= curr.VALID_FROM
    WHERE curr.BUSINESS_KEY IS NULL
       OR (src.ENTRY_TIMESTAMP > curr.VALID_FROM AND src.NAME_SPCM_HASH != curr.NAME_SPCM_HASH)
),

-- Combine expired and new records
final_updates AS (
    -- Expired records
    SELECT
        SURROGATE_KEY,
        BUSINESS_KEY,
        VALID_FROM,
        VALID_TO,
        IS_CURRENT,
        NULL as new_SURROGATE_KEY,
        NULL as ENTRY_TIMESTAMP,
        NULL as CHANGE_RANK,
        NULL as SEQUENCE_NUMBER,
        NULL as OPERATION,
        NULL as M0SLRP,
        NULL as M0NAME,
        NULL as M0SPCM,
        NULL as SOURCE_SYSTEM,
        NULL as SOURCE_FILE_NAME,
        NULL as BATCH_ID,
        NULL as RECORD_CHECKSUM_HASH,
        NULL as ETL_VERSION,
        NULL as INGESTION_DTTM,
        NULL as INGESTION_DT,
        NULL as NAME_SPCM_HASH
    FROM records_to_expire
    
    UNION ALL
    
    -- New records
    SELECT
        new_SURROGATE_KEY as SURROGATE_KEY,
        BUSINESS_KEY,
        ENTRY_TIMESTAMP as VALID_FROM,
        NULL as VALID_TO,
        TRUE as IS_CURRENT,
        new_SURROGATE_KEY,
        ENTRY_TIMESTAMP,
        CHANGE_RANK,
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
        NAME_SPCM_HASH
    FROM new_records
)
{% endif %}

-- Final output
SELECT
    {% if is_incremental() %}
        fu.SURROGATE_KEY,
        fu.VALID_FROM,
        fu.VALID_TO,
        fu.IS_CURRENT,
        COALESCE(fu.ENTRY_TIMESTAMP, swk.ENTRY_TIMESTAMP) as ENTRY_TIMESTAMP,
        COALESCE(fu.SEQUENCE_NUMBER, swk.SEQUENCE_NUMBER) as SEQUENCE_NUMBER,
        COALESCE(fu.OPERATION, swk.OPERATION) as OPERATION,
        COALESCE(fu.M0SLRP, swk.M0SLRP) as M0SLRP,
        COALESCE(fu.M0NAME, swk.M0NAME) as M0NAME,
        COALESCE(fu.M0SPCM, swk.M0SPCM) as M0SPCM,
        COALESCE(fu.SOURCE_SYSTEM, swk.SOURCE_SYSTEM) as SOURCE_SYSTEM,
        COALESCE(fu.SOURCE_FILE_NAME, swk.SOURCE_FILE_NAME) as SOURCE_FILE_NAME,
        COALESCE(fu.BATCH_ID, swk.BATCH_ID) as BATCH_ID,
        COALESCE(fu.RECORD_CHECKSUM_HASH, swk.RECORD_CHECKSUM_HASH) as RECORD_CHECKSUM_HASH,
        COALESCE(fu.ETL_VERSION, swk.ETL_VERSION) as ETL_VERSION,
        COALESCE(fu.INGESTION_DTTM, swk.INGESTION_DTTM) as INGESTION_DTTM,
        COALESCE(fu.INGESTION_DT, swk.INGESTION_DT) as INGESTION_DT,
        COALESCE(fu.NAME_SPCM_HASH, swk.NAME_SPCM_HASH) as NAME_SPCM_HASH,
        CURRENT_TIMESTAMP() as dbt_updated_at,
        'DBT' as dbt_updated_by,
        COALESCE(fu.CHANGE_RANK, swk.CHANGE_RANK) as CHANGE_RANK
    {% else %}
        ROW_NUMBER() OVER (ORDER BY swk.BUSINESS_KEY, swk.ENTRY_TIMESTAMP) as SURROGATE_KEY,
        swk.ENTRY_TIMESTAMP as VALID_FROM,
        NULL as VALID_TO,
        TRUE as IS_CURRENT,
        swk.ENTRY_TIMESTAMP,
        swk.SEQUENCE_NUMBER,
        swk.OPERATION,
        swk.M0SLRP,
        swk.M0NAME,
        swk.M0SPCM,
        swk.SOURCE_SYSTEM,
        swk.SOURCE_FILE_NAME,
        swk.BATCH_ID,
        swk.RECORD_CHECKSUM_HASH,
        swk.ETL_VERSION,
        swk.INGESTION_DTTM,
        swk.INGESTION_DT,
        swk.NAME_SPCM_HASH,
        CURRENT_TIMESTAMP() as dbt_updated_at,
        'DBT' as dbt_updated_by,
        swk.CHANGE_RANK
    {% endif %}
FROM 
{% if is_incremental() %}
    final_updates fu
    LEFT JOIN source_with_keys swk
        ON fu.BUSINESS_KEY = swk.BUSINESS_KEY
        AND (
            (fu.IS_CURRENT = TRUE AND fu.VALID_FROM = swk.ENTRY_TIMESTAMP) OR
            (fu.IS_CURRENT = FALSE)
        )
    WHERE fu.SURROGATE_KEY IS NOT NULL
{% else %}
    source_with_keys swk
{% endif %}
