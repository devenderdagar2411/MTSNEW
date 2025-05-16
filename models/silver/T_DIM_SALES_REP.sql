{{ config(
    materialized = 'incremental',
    unique_key = ['SURROGATE_KEY']
) }}

-- Step 1: Load new data from source
WITH source_data AS (
    SELECT        
        M0SLRP AS SALES_REP_NUMBER,
        M0NAME AS SALES_REP_NAME,
        M0SPCM AS SPIFF_COMMISSION_FLAG,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        INGESTION_DT,
        INGESTION_DTTM,
        ENTRY_TIMESTAMP
    FROM RAW_DATA.BRONZE_SALES.T_BRZ_SALESREP_MASTER_SASLRM
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (
            SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }}
        )
    {% endif %}
),

-- Step 2: Deduplication
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY SALES_REP_NUMBER, ENTRY_TIMESTAMP
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect changes based on RECORD_CHECKSUM_HASH
source_with_lag AS (
    SELECT
        curr.*,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY SALES_REP_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL
),

-- Step 4: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(SALES_REP_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY SALES_REP_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: Generate new rows
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.SALES_REP_NUMBER, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS SALES_REP_SK,
        oc.SALES_REP_NUMBER,
        oc.SALES_REP_NAME,
        oc.SPIFF_COMMISSION_FLAG,
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        oc.INGESTION_DTTM,
        oc.INGESTION_DT,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE NULL
        END AS EXPIRATION_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS IS_CURRENT_FLAG
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} tgt
        WHERE tgt.SALES_REP_NUMBER = oc.SALES_REP_NUMBER
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
    )
),

-- Step 7: Expire old current rows
expired_rows AS (
    SELECT
        old.SALES_REP_SK,
        old.SALES_REP_NUMBER,
        old.SALES_REP_NAME,
        old.SPIFF_COMMISSION_FLAG,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM_HASH,
        old.ETL_VERSION,
        old.INGESTION_DTTM,
        old.INGESTION_DT,
        old.EFFECTIVE_DATE,
        new.EFFECTIVE_DATE - INTERVAL '1 second' AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG
    FROM {{ this }} old
    JOIN new_rows new
      ON old.SALES_REP_NUMBER = new.SALES_REP_NUMBER
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
)

-- Step 8: Final Output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
