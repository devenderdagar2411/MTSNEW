{{ config(
    materialized = 'incremental',
    unique_key = ['INVENTORY_CATEGORY_SK']
) }}

WITH source_data AS (
    SELECT        
        CAST(TRIM(C1CTCD) AS NUMBER(3, 0)) AS CATEGORY_ID,
        CAST(TRIM(C1NAME) AS VARCHAR(40)) AS CATEGORY_NAME,
        CAST(TRIM(C1PCDP) AS NUMBER(3, 0)) AS PC_DEPARTMENT_CODE,
        CAST(TRIM(C1PGCD) AS NUMBER(3, 0)) AS PC_GROUP_CODE,
        CAST(TRIM(C1MCGP) AS NUMBER(3, 0)) AS MECHANIC_GROUP_CODE,
        CAST(TRIM(C1MCDN) AS NUMBER(3, 0)) AS MECHANIC_DETAIL_NUMBER,
        CAST(TRIM(C1CSRQ) AS VARCHAR(1)) AS COST_REQUIRED_AT_POS_FLAG,
        CAST(TRIM(C1DOAL) AS VARCHAR(1)) AS DOT_NUMBER_ALLOWED_AT_POS_FLAG,
        CAST(TRIM(C1STTF) AS VARCHAR(1)) AS STATE_TIRE_FEE_FLAG,
        CAST(TRIM(C1LG95) AS VARCHAR(1)) AS LOGIC_95_CENTS_FLAG,
        CAST(TRIM(C1SFL4) AS VARCHAR(1)) AS SIZE_SEARCH_FLIP_4_FLAG,
        CAST(TRIM(C1MGIO) AS VARCHAR(1)) AS MILEAGE_IN_OUT_PRINT_FLAG,
        CAST(TRIM(C1HRIO) AS VARCHAR(1)) AS HOURS_IN_OUT_PRINT_FLAG,
        CAST(TRIM(C1PQTY) AS VARCHAR(1)) AS PRECISION_QUANTITY_ENTRY_FLAG,
        CAST(TRIM(C1PPNA) AS VARCHAR(1)) AS POS_PRICE_AT_NAB_FLAG,
        CAST(TRIM(C1SZRQ) AS VARCHAR(1)) AS SIZE_REQUIRED_AT_POS_FLAG,
        CAST(TRIM(C1DSRQ) AS VARCHAR(1)) AS DESCRIPTION_REQUIRED_AT_POS_FLAG,
        CAST(TRIM(C1NOAV) AS VARCHAR(1)) AS DO_NOT_AVERAGE_COST_FLAG,
        CAST(TRIM(C1RDHZ) AS VARCHAR(1)) AS ROAD_HAZARD_FLAG,
        CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(100)) AS SOURCE_SYSTEM,
        CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(255)) AS SOURCE_FILE_NAME,
        CAST(TRIM(BATCH_ID) AS VARCHAR(100)) AS BATCH_ID,
        CAST(TRIM(ETL_VERSION) AS VARCHAR(50)) AS ETL_VERSION,
        CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(C1NAME), ''),
            COALESCE(TRIM(C1PCDP), ''),
            COALESCE(TRIM(C1PGCD), ''),
            COALESCE(TRIM(C1MCGP), ''),
            COALESCE(TRIM(C1MCDN), ''),
            COALESCE(TRIM(C1CSRQ), ''),
            COALESCE(TRIM(C1DOAL), ''),
            COALESCE(TRIM(C1STTF), ''),
            COALESCE(TRIM(C1LG95), ''),
            COALESCE(TRIM(C1SFL4), ''),
            COALESCE(TRIM(C1MGIO), ''),
            COALESCE(TRIM(C1HRIO), ''),
            COALESCE(TRIM(C1PQTY), ''),
            COALESCE(TRIM(C1PPNA), ''),
            COALESCE(TRIM(C1SZRQ), ''),
            COALESCE(TRIM(C1DSRQ), ''),
            COALESCE(TRIM(C1NOAV), ''),
            COALESCE(TRIM(C1RDHZ), '')
        )) AS RECORD_CHECKSUM_HASH,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_INV_CAT_INCATG') }}
    {% if is_incremental() %}
        WHERE ENTRY_TIMESTAMP > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }})
    {% endif %}
),

ranked_source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CATEGORY_ID
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
            PARTITION BY CATEGORY_ID ORDER BY ENTRY_TIMESTAMP
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
    SELECT COALESCE(MAX(INVENTORY_CATEGORY_SK), 0) AS max_sk FROM {{ this }}
),

ordered_changes AS (
    SELECT *,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY CATEGORY_ID ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.CATEGORY_ID, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS INVENTORY_CATEGORY_SK,
        oc.CATEGORY_ID,
        oc.CATEGORY_NAME,
        oc.PC_DEPARTMENT_CODE,
        oc.PC_GROUP_CODE,
        oc.MECHANIC_GROUP_CODE,
        oc.MECHANIC_DETAIL_NUMBER,
        oc.COST_REQUIRED_AT_POS_FLAG,
        oc.DOT_NUMBER_ALLOWED_AT_POS_FLAG,
        oc.STATE_TIRE_FEE_FLAG,
        oc.LOGIC_95_CENTS_FLAG,
        oc.SIZE_SEARCH_FLIP_4_FLAG,
        oc.MILEAGE_IN_OUT_PRINT_FLAG,
        oc.HOURS_IN_OUT_PRINT_FLAG,
        oc.PRECISION_QUANTITY_ENTRY_FLAG,
        oc.POS_PRICE_AT_NAB_FLAG,
        oc.SIZE_REQUIRED_AT_POS_FLAG,
        oc.DESCRIPTION_REQUIRED_AT_POS_FLAG,
        oc.DO_NOT_AVERAGE_COST_FLAG,
        oc.ROAD_HAZARD_FLAG,
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        CASE
            WHEN oc.next_entry_ts IS NOT NULL THEN oc.next_entry_ts - INTERVAL '1 second'
            ELSE '9999-12-31 23:59:59'::TIMESTAMP_NTZ
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
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM ordered_changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE tgt.CATEGORY_ID = oc.CATEGORY_ID
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

expired_rows AS (
    SELECT
        old.INVENTORY_CATEGORY_SK,
        old.CATEGORY_ID,
        old.CATEGORY_NAME,
        old.PC_DEPARTMENT_CODE,
        old.PC_GROUP_CODE,
        old.MECHANIC_GROUP_CODE,
        old.MECHANIC_DETAIL_NUMBER,
        old.COST_REQUIRED_AT_POS_FLAG,
        old.DOT_NUMBER_ALLOWED_AT_POS_FLAG,
        old.STATE_TIRE_FEE_FLAG,
        old.LOGIC_95_CENTS_FLAG,
        old.SIZE_SEARCH_FLIP_4_FLAG,
        old.MILEAGE_IN_OUT_PRINT_FLAG,
        old.HOURS_IN_OUT_PRINT_FLAG,
        old.PRECISION_QUANTITY_ENTRY_FLAG,
        old.POS_PRICE_AT_NAB_FLAG,
        old.SIZE_REQUIRED_AT_POS_FLAG,
        old.DESCRIPTION_REQUIRED_AT_POS_FLAG,
        old.DO_NOT_AVERAGE_COST_FLAG,
        old.ROAD_HAZARD_FLAG,
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
      ON old.CATEGORY_ID = new.CATEGORY_ID
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

soft_deleted_rows AS (
    SELECT
        old.INVENTORY_CATEGORY_SK,
        old.CATEGORY_ID,
        old.CATEGORY_NAME,
        old.PC_DEPARTMENT_CODE,
        old.PC_GROUP_CODE,
        old.MECHANIC_GROUP_CODE,
        old.MECHANIC_DETAIL_NUMBER,
        old.COST_REQUIRED_AT_POS_FLAG,
        old.DOT_NUMBER_ALLOWED_AT_POS_FLAG,
        old.STATE_TIRE_FEE_FLAG,
        old.LOGIC_95_CENTS_FLAG,
        old.SIZE_SEARCH_FLIP_4_FLAG,
        old.MILEAGE_IN_OUT_PRINT_FLAG,
        old.HOURS_IN_OUT_PRINT_FLAG,
        old.PRECISION_QUANTITY_ENTRY_FLAG,
        old.POS_PRICE_AT_NAB_FLAG,
        old.SIZE_REQUIRED_AT_POS_FLAG,
        old.DESCRIPTION_REQUIRED_AT_POS_FLAG,
        old.DO_NOT_AVERAGE_COST_FLAG,
        old.ROAD_HAZARD_FLAG,
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
      ON old.CATEGORY_ID = del.CATEGORY_ID
     AND old.IS_CURRENT_FLAG = TRUE
)

SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows
