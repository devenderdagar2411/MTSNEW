{{ config(
    materialized = 'incremental',
    unique_key = ['SURROGATE_KEY']
) }}

-- Step 1: Load new data
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
        INGESTION_DATE,
        INGESTION_TIMESTAMP,
        ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(M0SLRP::VARCHAR, ''),
            COALESCE(M0NAME::VARCHAR, ''),
            COALESCE(M0SPCM::VARCHAR, '')
        )) AS TRACKING_HASH
    FROM RAW_DATA.BRONZE_SALES.T_BRZ_SALESREP_MASTER_SASLR_CLONE
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (
            SELECT COALESCE(MAX(VALID_FROM), '1900-01-01') FROM {{ this }}
        )
    {% endif %}
),

-- Step 2: Deduplicate if necessary
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY M0SLRP
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),

deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect changes
source_with_lag AS (
    SELECT
        curr.*,
        LAG(TRACKING_HASH) OVER (
            PARTITION BY M0SLRP ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),

changes AS (
    SELECT *
    FROM source_with_lag
    WHERE TRACKING_HASH != prev_hash OR prev_hash IS NULL
),

-- Step 4: Get the max SK
max_key AS (
    SELECT COALESCE(MAX(SURROGATE_KEY), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate next ENTRY_TIMESTAMP to chain VALID_TO
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY M0SLRP
            ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: Generate new version rows
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.M0SLRP, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk   AS SURROGATE_KEY,
        oc.M0SLRP,
        oc.M0NAME,
        oc.M0SPCM,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM,
        oc.ETL_VERSION,
        oc.INGESTION_DATE,
        oc.INGESTION_TIMESTAMP,
        oc.TRACKING_HASH,
        oc.ENTRY_TIMESTAMP AS VALID_FROM,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE NULL
        END AS VALID_TO,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS IS_CURRENT,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} tgt
        WHERE tgt.M0SLRP = oc.M0SLRP
          AND tgt.TRACKING_HASH = oc.TRACKING_HASH
          AND tgt.VALID_FROM = oc.ENTRY_TIMESTAMP
          and IS_CURRENT=TRUE
    )
),

-- Step 7: Expire old active rows
expired_rows AS (
    SELECT
        old.SURROGATE_KEY,
        old.M0SLRP,
        old.M0NAME,
        old.M0SPCM,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM,
        old.ETL_VERSION,
        old.INGESTION_DATE,
        old.INGESTION_TIMESTAMP,
        old.TRACKING_HASH,
        old.VALID_FROM,
        new.VALID_FROM - INTERVAL '1 second' AS VALID_TO,
        FALSE AS IS_CURRENT,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT,
        'DBT' AS DBT_UPDATED_BY
    FROM {{ this }} old
    JOIN new_rows new
      ON old.M0SLRP = new.M0SLRP
     AND old.IS_CURRENT = TRUE
     AND old.TRACKING_HASH != new.TRACKING_HASH
)

-- Step 8: Final output to be inserted
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
