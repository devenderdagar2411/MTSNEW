{{ config(
    materialized = 'incremental',
    unique_key = ['FORM_TYPE_SK']
) }}

WITH source_data AS (
    SELECT        
        CAST(TRIM(W0FMTP) AS VARCHAR(2)) AS FORM_TYPE_CODE,
        CAST(TRIM(W0NAME) AS VARCHAR(40)) AS FORM_TYPE_NAME,
        CAST(TRIM(W0DRBL) AS VARCHAR(1)) AS DIRECT_BILLING_FLAG,
        CAST(TRIM(W0POSS) AS VARCHAR(1)) AS POS_SELECT_FLAG,
        CAST(TRIM(W0MCRQ) AS VARCHAR(1)) AS MECHANIC_REQUIRED_FLAG,
        CAST(TRIM(W0CSRQ) AS VARCHAR(1)) AS COST_REQUIRED_FLAG,
        CAST(TRIM(W0PRPR) AS VARCHAR(1)) AS PRINT_PRICING_FLAG,
        CAST(TRIM(W0PRAM) AS VARCHAR(1)) AS PRINT_INVOICE_AMOUNT_FLAG,
        CAST(TRIM(W0CM) AS VARCHAR(1)) AS CREDIT_MEMO_FLAG,
        CAST(TRIM(W0SFEE) AS VARCHAR(1)) AS STATE_TIRE_FEE_FLAG,
        CAST(TRIM(W0IFMT) AS VARCHAR(2)) AS INVOICING_FORMTYPE,
        CAST(TRIM(W0PDOT) AS VARCHAR(1)) AS PAID_OUT_FLAG,
        CAST(TRIM(W0DR) AS VARCHAR(1)) AS DR_SUBSYSTEM,
        CAST(TRIM(W0CL) AS VARCHAR(1)) AS CREDIT_LIMIT_CHECKING_FLAG,
        CAST(TRIM(W0QUOT) AS VARCHAR(1)) AS QUOTE_FLAG,
        CAST(TRIM(W0QTIV) AS VARCHAR(1)) AS QUOTE_PRINT_AS_INVOICE_FLAG,
        CAST(TRIM(W0PHRQ) AS VARCHAR(1)) AS PHONE_NUMBER_REQUIRED_FLAG,
        CAST(TRIM(W0RDHZ) AS VARCHAR(1)) AS ROAD_HAZARD_PROTECTION_FLAG,
        CAST(TRIM(W0DOTR) AS VARCHAR(1)) AS DOT_NUMBER_REQUIRED_FLAG,
        CAST(TRIM(W0IVCE) AS VARCHAR(1)) AS INVOICE_ENTRY_REQUIRED_FLAG,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(W0NAME), ''),
            COALESCE(TRIM(W0DRBL), ''),
            COALESCE(TRIM(W0POSS), ''),
            COALESCE(TRIM(W0MCRQ), ''),
            COALESCE(TRIM(W0CSRQ), ''),
            COALESCE(TRIM(W0PRPR), ''),
            COALESCE(TRIM(W0PRAM), ''),
            COALESCE(TRIM(W0CM), ''),
            COALESCE(TRIM(W0SFEE), ''),
            COALESCE(TRIM(W0IFMT), ''),
            COALESCE(TRIM(W0PDOT), ''),
            COALESCE(TRIM(W0DR), ''),
            COALESCE(TRIM(W0CL), ''),
            COALESCE(TRIM(W0QUOT), ''),
            COALESCE(TRIM(W0QTIV), ''),
            COALESCE(TRIM(W0PHRQ), ''),
            COALESCE(TRIM(W0RDHZ), ''),
            COALESCE(TRIM(W0DOTR), ''),
            COALESCE(TRIM(W0IVCE), '')
        )) AS RECORD_CHECKSUM_HASH,        
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_FORMTYPE_WOFMTP') }}
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
            PARTITION BY FORM_TYPE_CODE
            ORDER BY ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_data
),
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 3: Detect inserts/updates
source_with_lag AS (
    SELECT
        curr.*,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY FORM_TYPE_CODE ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE (RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL)
      AND OPERATION != 'DELETE'
),

-- Step 3b: Detect deletes
deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

-- Step 4: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(FORM_TYPE_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 5: Calculate future expiration dates for changes
ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY FORM_TYPE_CODE ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

-- Step 6: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.FORM_TYPE_CODE, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS FORM_TYPE_SK,
        oc.FORM_TYPE_CODE,
        oc.FORM_TYPE_NAME,
        oc.DIRECT_BILLING_FLAG,
        oc.POS_SELECT_FLAG,
        oc.MECHANIC_REQUIRED_FLAG,
        oc.COST_REQUIRED_FLAG,
        oc.PRINT_PRICING_FLAG,
        oc.PRINT_INVOICE_AMOUNT_FLAG,
        oc.CREDIT_MEMO_FLAG,
        oc.STATE_TIRE_FEE_FLAG,
        oc.INVOICING_FORMTYPE,
        oc.PAID_OUT_FLAG,
        oc.DR_SUBSYSTEM,
        oc.CREDIT_LIMIT_CHECKING_FLAG,
        oc.QUOTE_FLAG,
        oc.QUOTE_PRINT_AS_INVOICE_FLAG,
        oc.PHONE_NUMBER_REQUIRED_FLAG,
        oc.ROAD_HAZARD_PROTECTION_FLAG,
        oc.DOT_NUMBER_REQUIRED_FLAG,
        oc.INVOICE_ENTRY_REQUIRED_FLAG,
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
        WHERE tgt.FORM_TYPE_CODE = oc.FORM_TYPE_CODE
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old current rows when updates happen
expired_rows AS (
    SELECT
        old.FORM_TYPE_SK,
        old.FORM_TYPE_CODE,
        old.FORM_TYPE_NAME,
        old.DIRECT_BILLING_FLAG,
        old.POS_SELECT_FLAG,
        old.MECHANIC_REQUIRED_FLAG,
        old.COST_REQUIRED_FLAG,
        old.PRINT_PRICING_FLAG,
        old.PRINT_INVOICE_AMOUNT_FLAG,
        old.CREDIT_MEMO_FLAG,
        old.STATE_TIRE_FEE_FLAG,
        old.INVOICING_FORMTYPE,
        old.PAID_OUT_FLAG,
        old.DR_SUBSYSTEM,
        old.CREDIT_LIMIT_CHECKING_FLAG,
        old.QUOTE_FLAG,
        old.QUOTE_PRINT_AS_INVOICE_FLAG,
        old.PHONE_NUMBER_REQUIRED_FLAG,
        old.ROAD_HAZARD_PROTECTION_FLAG,
        old.DOT_NUMBER_REQUIRED_FLAG,
        old.INVOICE_ENTRY_REQUIRED_FLAG,
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
      ON old.FORM_TYPE_CODE = new.FORM_TYPE_CODE
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes - expire records if source says DELETE
soft_deletes AS (
    SELECT
        old.FORM_TYPE_SK,
        old.FORM_TYPE_CODE,
        old.FORM_TYPE_NAME,
        old.DIRECT_BILLING_FLAG,
        old.POS_SELECT_FLAG,
        old.MECHANIC_REQUIRED_FLAG,
        old.COST_REQUIRED_FLAG,
        old.PRINT_PRICING_FLAG,
        old.PRINT_INVOICE_AMOUNT_FLAG,
        old.CREDIT_MEMO_FLAG,
        old.STATE_TIRE_FEE_FLAG,
        old.INVOICING_FORMTYPE,
        old.PAID_OUT_FLAG,
        old.DR_SUBSYSTEM,
        old.CREDIT_LIMIT_CHECKING_FLAG,
        old.QUOTE_FLAG,
        old.QUOTE_PRINT_AS_INVOICE_FLAG,
        old.PHONE_NUMBER_REQUIRED_FLAG,
        old.ROAD_HAZARD_PROTECTION_FLAG,
        old.DOT_NUMBER_REQUIRED_FLAG,
        old.INVOICE_ENTRY_REQUIRED_FLAG,
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
      ON old.FORM_TYPE_CODE = del.FORM_TYPE_CODE
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deletes
UNION ALL
SELECT * FROM new_rows
