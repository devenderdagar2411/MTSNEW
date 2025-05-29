{{ config(
    materialized = 'incremental',
    unique_key = 'INVENTORY_CLASS_SK'
) }}
 
WITH source_data AS (
    SELECT
        TRY_CAST(TRIM(C2CLCD) AS INTEGER) AS CLASS_ID,
        TRIM(C2NAME) AS CLASS_NAME,
        TRY_CAST(TRIM(C2CTCD) AS INTEGER) AS CATEGORY_CODE,
        TRY_CAST(TRIM(C2IVDN) AS INTEGER) AS INVENTORY_VENDOR_NUMBER,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(C2NAME), ''),
            COALESCE(TRIM(C2CTCD), ''),
            COALESCE(TRIM(C2IVDN), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM RAW_DATA.BRONZE_SALES.T_BRZ_INV_CLASS_INCLAS
     {% if is_incremental() %}   
     WHERE ENTRY_TIMESTAMP ='1900-01-01T00:00:00Z'
        --WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }})
    {% endif %}
),
 
ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CLASS_ID ORDER BY ENTRY_TIMESTAMP DESC) AS rn
    FROM source_data
),
 
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),
 
changes AS (
    SELECT * FROM deduplicated_source WHERE OPERATION != 'DELETE'
),
 
deletes AS (
    SELECT * FROM deduplicated_source WHERE OPERATION = 'DELETE'
),
 
max_key AS (
    SELECT COALESCE(MAX(INVENTORY_CLASS_SK), 0) AS max_sk FROM {{ this }}
),
 
changes_with_category AS (
    SELECT
        c.*,
        ic.INVENTORY_CATEGORY_SK
    FROM changes c    
    LEFT JOIN ANALYTICS.SILVER_SALES.T_DIM_INVENTORY_CATEGORY ic
    ON c.CATEGORY_CODE = ic.CATEGORY_ID
     LEFT JOIN {{ ref('T_DIM_INVENTORY_VENDOR') }} dim
      ON dim.VENDOR_NUMBER = oc.INVENTORY_VENDOR_NUMBER
     AND oc.ENTRY_TIMESTAMP BETWEEN dim.EFFECTIVE_DATE AND COALESCE(dim.EXPIRATION_DATE, '9999-12-31')
),
 
ordered_changes AS (
    SELECT
        cwc.*,
        LEAD(ENTRY_TIMESTAMP) OVER (PARTITION BY CLASS_ID ORDER BY ENTRY_TIMESTAMP) AS next_entry_ts
    FROM changes_with_category cwc
),
 
new_rows AS (
    SELECT
        CAST(ROW_NUMBER() OVER (ORDER BY oc.CLASS_ID, oc.ENTRY_TIMESTAMP) + max_key.max_sk AS NUMBER(38,0)) AS INVENTORY_CLASS_SK,
        oc.CLASS_ID,
        oc.CLASS_NAME,
        oc.CATEGORY_CODE,
        oc.INVENTORY_VENDOR_NUMBER,
        oc.INVENTORY_CATEGORY_SK,
        oc.LAST_MAINTAINED_USER,
        oc.LAST_MAINTAINED_DATE,  -- keep as number YYYYMMDD
        oc.LAST_MAINTAINED_TIME,
        oc.LAST_MAINTAINED_WORKSTATION,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        COALESCE(oc.next_entry_ts - INTERVAL '1 SECOND', TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')) AS EXPIRATION_DATE,
        CASE WHEN oc.next_entry_ts IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT_FLAG,
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
        SELECT 1 FROM {{ this }} tgt
        WHERE tgt.CLASS_ID = oc.CLASS_ID
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),
 
expired_rows AS (
    SELECT
        old.INVENTORY_CLASS_SK,
        old.CLASS_ID,
        old.CLASS_NAME,
        old.CATEGORY_CODE,
        old.INVENTORY_VENDOR_NUMBER,
        old.INVENTORY_CATEGORY_SK,
        old.LAST_MAINTAINED_USER,
        old.LAST_MAINTAINED_DATE,
        old.LAST_MAINTAINED_TIME,
        old.LAST_MAINTAINED_WORKSTATION,
        old.EFFECTIVE_DATE,
        new.EFFECTIVE_DATE - INTERVAL '1 SECOND' AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM_HASH,
        old.ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM {{ this }} old
    JOIN new_rows new
      ON old.CLASS_ID = new.CLASS_ID
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),
 
soft_deleted_rows AS (
    SELECT
        old.INVENTORY_CLASS_SK,
        old.CLASS_ID,
        old.CLASS_NAME,
        old.CATEGORY_CODE,
        old.INVENTORY_VENDOR_NUMBER,
        old.INVENTORY_CATEGORY_SK,
        old.LAST_MAINTAINED_USER,
        old.LAST_MAINTAINED_DATE,
        old.LAST_MAINTAINED_TIME,
        old.LAST_MAINTAINED_WORKSTATION,
        old.EFFECTIVE_DATE,
        del.ENTRY_TIMESTAMP AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        old.SOURCE_SYSTEM,
        old.SOURCE_FILE_NAME,
        old.BATCH_ID,
        old.RECORD_CHECKSUM_HASH,
        old.ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM {{ this }} old
    JOIN deletes del
      ON old.CLASS_ID = del.CLASS_ID
     AND old.IS_CURRENT_FLAG = TRUE
)
 
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows