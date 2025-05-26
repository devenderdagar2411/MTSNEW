{{ config(
    materialized = 'incremental',
    unique_key = ['PRODUCT_SK']
) }}

WITH source_data AS (
    SELECT
        CAST(TRIM(I0ITM) AS INTEGER) AS ITEM_NUMBER,
        CAST(TRIM(I0DESC) AS VARCHAR(40)) AS ITEM_DESCRIPTION,
        CAST(TRIM(I0CLCD) AS INTEGER) AS CLASS_CODE,
        CAST(TRIM(I0PROD) AS VARCHAR(11)) AS PRODUCT_CODE,
        CAST(TRIM(I0SIZE) AS VARCHAR(20)) AS SIZE,
        CAST(TRIM(I0PRSZ) AS VARCHAR(3)) AS PRODUCT_SIZE,
        CAST(TRIM(I0PLY) AS VARCHAR(5)) AS PLY,
        CAST(TRIM(I0MFCD) AS VARCHAR(20)) AS MANUFACTURER_CODE,
        CAST(TRIM(I0TXCD) AS INTEGER) AS TAX_CODE,
        CAST(TRIM(I0WGHT) AS NUMERIC(9,2)) AS WEIGHT,
        CAST(TRIM(I0FDTX) AS NUMERIC(9,2)) AS FEDERAL_TAX,
        CAST(TRIM(I0BSCS) AS NUMERIC(9,2)) AS BASE_COST,
        CAST(TRIM(I0NTBC) AS NUMERIC(9,2)) AS NET_BILL_COST,
        CAST(TRIM(I0UNCS) AS NUMERIC(9,2)) AS UNIT_COST,
        CAST(TRIM(I0AVCS) AS NUMERIC(9,2)) AS AVERAGE_COST,
        CAST(TRIM(I0FRCS) AS NUMERIC(9,2)) AS FREIGHT_COST,
        CAST(TRIM(I0CSCS) AS NUMERIC(9,2)) AS CASING_COST,
        CAST(TRIM(I0CS01) AS NUMERIC(9,2)) AS MISC_COST_01,
        CAST(TRIM(I0CS02) AS NUMERIC(9,2)) AS MISC_COST_02,
        CAST(TRIM(I0CS03) AS NUMERIC(9,2)) AS MISC_COST_03,
        CAST(TRIM(I0CS04) AS NUMERIC(9,2)) AS MISC_COST_04,
        CAST(TRIM(I0CS05) AS NUMERIC(9,2)) AS MISC_COST_05,
        CAST(TRIM(I0CS06) AS NUMERIC(9,2)) AS MISC_COST_06,
        CAST(TRIM(I0CS07) AS NUMERIC(9,2)) AS MISC_COST_07,
        CAST(TRIM(I0CS08) AS NUMERIC(9,2)) AS MISC_COST_08,
        CAST(TRIM(I0CS09) AS NUMERIC(9,2)) AS MISC_COST_09,
        CAST(TRIM(I0CS10) AS NUMERIC(9,2)) AS MISC_COST_10,
        CAST(TRIM(I0IVDN) AS INTEGER) AS INVENTORY_VENDOR_NUMBER,
        CAST(TRIM(I0GPCD) AS INTEGER) AS GP_CODE,
        CAST(TRIM(I0PCCD) AS VARCHAR(1)) AS PC_SWITCH,
        CAST(TRIM(I0SBCD) AS INTEGER) AS SUB_CODE,
        CAST(TRIM(I0LPDT) AS INTEGER) AS LAST_PRICE_CHANGE_DATE,
        CAST(TRIM(I0SFCD) AS INTEGER) AS SPIFF_CODE,
        CAST(TRIM(I0EQOH) AS VARCHAR(1)) AS EFFECTS_QOH_FLAG,
        CAST(TRIM(I0SRSZ) AS VARCHAR(20)) AS ALPHA_SEARCH_SIZE,
        CAST(TRIM("I0SRS#") AS VARCHAR(20)) AS NUMERIC_SEARCH_SIZE,
        CAST(TRIM(I0CTCD) AS INTEGER) AS CATEGORY_CODE,
        CAST(TRIM(I0PRC1) AS NUMERIC(9,3)) AS PRICE_1,
        CAST(TRIM(I0PRC2) AS NUMERIC(9,3)) AS PRICE_2,
        CAST(TRIM(I0DSIT) AS VARCHAR(1)) AS DISCONTINUED_ITEM_FLAG,
        CAST(TRIM(I0CSIT) AS VARCHAR(1)) AS CONSIGNMENT_ITEM_FLAG,
        CAST(TRIM(I0CSRQ) AS VARCHAR(1)) AS COST_REQUIRED_AT_POS_FLAG,
        -- Assuming the following columns exist in source or are defaulted (set to NULL or appropriate defaults)
        NULL::VARCHAR AS DO_NOT_AVERAGE_COST_FLAG,
        NULL::VARCHAR AS ROAD_HAZARD_ITEM_FLAG,
        NULL::VARCHAR AS RETREAD_STOCK_FLAG,
        NULL::VARCHAR AS STATE_TIRE_FEE_EXEMPT_FLAG,
        NULL::VARCHAR AS REVENUE_STREAM_CODE,
        NULL::VARCHAR AS REVENUE_STREAM_NAME,
        NULL::INTEGER AS CLASS_SK,
        NULL::INTEGER AS CATEGORY_SK,
        NULL::VARCHAR AS LAST_MAINTAINED_USER,
        -- Corrected: If LAST_MAINTAINED_DATE is not in source, leave it as NULL::DATE
        NULL::DATE AS LAST_MAINTAINED_DATE,
        NULL::VARCHAR AS LAST_MAINTAINED_TIME,
        NULL::VARCHAR AS LAST_MAINTAINED_WORKSTATION,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP,
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(I0DESC), ''),
            COALESCE(TRIM(I0CLCD), ''),
            COALESCE(TRIM(I0PROD), ''),
            COALESCE(TRIM(I0SIZE), ''),
            COALESCE(TRIM(I0PRSZ), ''),
            COALESCE(TRIM(I0MFCD), ''),
            COALESCE(TRIM(I0WGHT), ''),
            COALESCE(TRIM(I0FDTX), '')
        )) AS RECORD_CHECKSUM_HASH,
        'SOURCE_SYSTEM_NAME' AS SOURCE_SYSTEM,
        'SOURCE_FILE_NAME' AS SOURCE_FILE_NAME,
        'BATCH_ID_VALUE' AS BATCH_ID,
        'ETL_VERSION_VALUE' AS ETL_VERSION
    FROM {{ source('bronze_data', 'T_BRZ_ITEM_MASTER_INITEM') }}
    {% if is_incremental() %}
        WHERE TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) > (
            SELECT COALESCE(MAX(EFFECTIVE_DATE), '1900-01-01') FROM {{ this }}
        )
    {% endif %}
),

ranked_source AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY ITEM_NUMBER ORDER BY ENTRY_TIMESTAMP DESC
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
            PARTITION BY ITEM_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),

changes AS (
    SELECT *
    FROM source_with_lag
    WHERE RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL
),

{% if is_incremental() %}
max_key AS (
    SELECT COALESCE(MAX(PRODUCT_SK), 0) AS max_sk FROM {{ this }}
),
{% else %}
max_key AS (
    SELECT 0 AS max_sk
),
{% endif %}

ordered_changes AS (
    SELECT * ,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY ITEM_NUMBER ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes
),

new_rows AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ITEM_NUMBER, ENTRY_TIMESTAMP) + max_key.max_sk AS PRODUCT_SK,
        ITEM_NUMBER,
        ITEM_DESCRIPTION,
        CLASS_CODE,
        PRODUCT_CODE,
        SIZE,
        PRODUCT_SIZE,
        PLY,
        MANUFACTURER_CODE,
        TAX_CODE,
        WEIGHT,
        FEDERAL_TAX,
        BASE_COST,
        NET_BILL_COST,
        UNIT_COST,
        AVERAGE_COST,
        FREIGHT_COST,
        CASING_COST,
        MISC_COST_01,
        MISC_COST_02,
        MISC_COST_03,
        MISC_COST_04,
        MISC_COST_05,
        MISC_COST_06,
        MISC_COST_07,
        MISC_COST_08,
        MISC_COST_09,
        MISC_COST_10,
        INVENTORY_VENDOR_NUMBER,
        GP_CODE,
        PC_SWITCH,
        SUB_CODE,
        LAST_PRICE_CHANGE_DATE,
        SPIFF_CODE,
        EFFECTS_QOH_FLAG,
        ALPHA_SEARCH_SIZE,
        NUMERIC_SEARCH_SIZE,
        CATEGORY_CODE,
        PRICE_1,
        PRICE_2,
        DISCONTINUED_ITEM_FLAG,
        CONSIGNMENT_ITEM_FLAG,
        COST_REQUIRED_AT_POS_FLAG,
        DO_NOT_AVERAGE_COST_FLAG,
        ROAD_HAZARD_ITEM_FLAG,
        RETREAD_STOCK_FLAG,
        STATE_TIRE_FEE_EXEMPT_FLAG,
        REVENUE_STREAM_CODE,
        REVENUE_STREAM_NAME,
        CLASS_SK,
        CATEGORY_SK,
        LAST_MAINTAINED_USER,
        -- Corrected: Since LAST_MAINTAINED_DATE is already DATE or NULL::DATE, no need for TO_TIMESTAMP_NTZ
        LAST_MAINTAINED_DATE AS LAST_MODIFIED_DATE,
        LAST_MAINTAINED_TIME,
        LAST_MAINTAINED_WORKSTATION,
        ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        COALESCE(next_entry_ts - INTERVAL '1 second', TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')) AS EXPIRATION_DATE,
        CASE WHEN next_entry_ts IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT_FLAG,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        RECORD_CHECKSUM_HASH,
        ETL_VERSION,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT
    FROM ordered_changes
    CROSS JOIN max_key
),

expired_rows AS (
    SELECT
        tgt.PRODUCT_SK,
        tgt.ITEM_NUMBER,
        tgt.ITEM_DESCRIPTION,
        tgt.CLASS_CODE,
        tgt.PRODUCT_CODE,
        tgt.SIZE,
        tgt.PRODUCT_SIZE,
        tgt.PLY,
        tgt.MANUFACTURER_CODE,
        tgt.TAX_CODE,
        tgt.WEIGHT,
        tgt.FEDERAL_TAX,
        tgt.BASE_COST,
        tgt.NET_BILL_COST,
        tgt.UNIT_COST,
        tgt.AVERAGE_COST,
        tgt.FREIGHT_COST,
        tgt.CASING_COST,
        tgt.MISC_COST_01,
        tgt.MISC_COST_02,
        tgt.MISC_COST_03,
        tgt.MISC_COST_04,
        tgt.MISC_COST_05,
        tgt.MISC_COST_06,
        tgt.MISC_COST_07,
        tgt.MISC_COST_08,
        tgt.MISC_COST_09,
        tgt.MISC_COST_10,
        tgt.INVENTORY_VENDOR_NUMBER,
        tgt.GP_CODE,
        tgt.PC_SWITCH,
        tgt.SUB_CODE,
        tgt.LAST_PRICE_CHANGE_DATE,
        tgt.SPIFF_CODE,
        tgt.EFFECTS_QOH_FLAG,
        tgt.ALPHA_SEARCH_SIZE,
        tgt.NUMERIC_SEARCH_SIZE,
        tgt.CATEGORY_CODE,
        tgt.PRICE_1,
        tgt.PRICE_2,
        tgt.DISCONTINUED_ITEM_FLAG,
        tgt.CONSIGNMENT_ITEM_FLAG,
        tgt.COST_REQUIRED_AT_POS_FLAG,
        tgt.DO_NOT_AVERAGE_COST_FLAG,
        tgt.ROAD_HAZARD_ITEM_FLAG,
        tgt.RETREAD_STOCK_FLAG,
        tgt.STATE_TIRE_FEE_EXEMPT_FLAG,
        tgt.REVENUE_STREAM_CODE,
        tgt.REVENUE_STREAM_NAME,
        tgt.CLASS_SK,
        tgt.CATEGORY_SK,
        tgt.LAST_MAINTAINED_USER,
        -- Corrected: Since LAST_MAINTAINED_DATE is already DATE or NULL::DATE, no need for TO_TIMESTAMP_NTZ
        tgt.LAST_MAINTAINED_DATE AS LAST_MODIFIED_DATE,
        tgt.LAST_MAINTAINED_TIME,
        tgt.LAST_MAINTAINED_WORKSTATION,
        tgt.EFFECTIVE_DATE,
        new.EFFECTIVE_DATE - INTERVAL '1 second' AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        tgt.SOURCE_SYSTEM,
        tgt.SOURCE_FILE_NAME,
        tgt.BATCH_ID,
        tgt.RECORD_CHECKSUM_HASH,
        tgt.ETL_VERSION,
        tgt.INGESTION_DTTM,
        tgt.INGESTION_DT
    FROM {{ this }} tgt
    JOIN new_rows new
      ON tgt.ITEM_NUMBER = new.ITEM_NUMBER
     AND tgt.IS_CURRENT_FLAG = TRUE
     AND new.EFFECTIVE_DATE > tgt.EFFECTIVE_DATE
),

soft_deleted_rows AS (
    SELECT
        tgt.PRODUCT_SK,
        tgt.ITEM_NUMBER,
        tgt.ITEM_DESCRIPTION,
        tgt.CLASS_CODE,
        tgt.PRODUCT_CODE,
        tgt.SIZE,
        tgt.PRODUCT_SIZE,
        tgt.PLY,
        tgt.MANUFACTURER_CODE,
        tgt.TAX_CODE,
        tgt.WEIGHT,
        tgt.FEDERAL_TAX,
        tgt.BASE_COST,
        tgt.NET_BILL_COST,
        tgt.UNIT_COST,
        tgt.AVERAGE_COST,
        tgt.FREIGHT_COST,
        tgt.CASING_COST,
        tgt.MISC_COST_01,
        tgt.MISC_COST_02,
        tgt.MISC_COST_03,
        tgt.MISC_COST_04,
        tgt.MISC_COST_05,
        tgt.MISC_COST_06,
        tgt.MISC_COST_07,
        tgt.MISC_COST_08,
        tgt.MISC_COST_09,
        tgt.MISC_COST_10,
        tgt.INVENTORY_VENDOR_NUMBER,
        tgt.GP_CODE,
        tgt.PC_SWITCH,
        tgt.SUB_CODE,
        tgt.LAST_PRICE_CHANGE_DATE,
        tgt.SPIFF_CODE,
        tgt.EFFECTS_QOH_FLAG,
        tgt.ALPHA_SEARCH_SIZE,
        tgt.NUMERIC_SEARCH_SIZE,
        tgt.CATEGORY_CODE,
        tgt.PRICE_1,
        tgt.PRICE_2,
        tgt.DISCONTINUED_ITEM_FLAG,
        tgt.CONSIGNMENT_ITEM_FLAG,
        tgt.COST_REQUIRED_AT_POS_FLAG,
        tgt.DO_NOT_AVERAGE_COST_FLAG,
        tgt.ROAD_HAZARD_ITEM_FLAG,
        tgt.RETREAD_STOCK_FLAG,
        tgt.STATE_TIRE_FEE_EXEMPT_FLAG,
        tgt.REVENUE_STREAM_CODE,
        tgt.REVENUE_STREAM_NAME,
        tgt.CLASS_SK,
        tgt.CATEGORY_SK,
        tgt.LAST_MAINTAINED_USER,
        -- Corrected: Since LAST_MAINTAINED_DATE is already DATE or NULL::DATE, no need for TO_TIMESTAMP_NTZ
        tgt.LAST_MAINTAINED_DATE AS LAST_MODIFIED_DATE,
        tgt.LAST_MAINTAINED_TIME,
        tgt.LAST_MAINTAINED_WORKSTATION,
        tgt.EFFECTIVE_DATE,
        CURRENT_TIMESTAMP() AS EXPIRATION_DATE,
        FALSE AS IS_CURRENT_FLAG,
        tgt.SOURCE_SYSTEM,
        tgt.SOURCE_FILE_NAME,
        tgt.BATCH_ID,
        tgt.RECORD_CHECKSUM_HASH,
        tgt.ETL_VERSION,
        tgt.INGESTION_DTTM,
        tgt.INGESTION_DT
    FROM {{ this }} tgt
    LEFT JOIN source_data src
      ON tgt.ITEM_NUMBER = src.ITEM_NUMBER
    WHERE src.ITEM_NUMBER IS NULL
      AND tgt.IS_CURRENT_FLAG = TRUE
)

SELECT * FROM expired_rows
UNION ALL
SELECT * FROM new_rows
UNION ALL
SELECT * FROM soft_deleted_rows