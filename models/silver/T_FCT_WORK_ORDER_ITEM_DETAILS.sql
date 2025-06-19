{{ config(
    materialized='incremental',
    unique_key='WORK_ORDER_ITEM_DETAILS_SK'
) }}

-- Step 1: Load and transform source data
WITH source_data AS (
    SELECT
        CAST(TRIM(src.W2STORE) AS NUMBER(3, 0)) AS STORE_NUMBER,
        CAST(TRIM(src.W2FMTP) AS VARCHAR(2)) AS FORM_TYPE_CODE,
        CAST(TRIM(src.W2WIPX) AS VARCHAR(3)) AS POS_PREFIX,
        CAST(TRIM(src.W2WO) AS NUMBER(10, 0)) AS WORK_ORDER_NUMBER,
        CAST(TRIM(src.W2SEQ) AS NUMBER(4, 0)) AS ITEM_SEQ_NUMBER,
        CAST(TRIM(src.W2ITM) AS NUMBER(10, 0)) AS PRODUCT_NUMBER,
        CAST(TRIM(src.W2MFCD) AS VARCHAR(20)) AS MANUFACTURING_CODE,
        CAST(TRIM(src.W2QTY) AS NUMBER(7, 0)) AS QUANTITY,
        CAST(TRIM(src.W2SIZE) AS VARCHAR(20)) AS SIZE,
        CAST(TRIM(src.W2DESC) AS VARCHAR(40)) AS DESCRIPTION,
        CAST(TRIM(src.W2PLY) AS VARCHAR(5)) AS PLY_RATING,
        CAST(TRIM(src.W2PRCD) AS NUMBER(3, 0)) AS PRICE_CODE,
        CAST(TRIM(src.W2UNCS) AS NUMBER(9, 2)) AS UNIT_COST,
        CAST(TRIM(src.W2UNPR) AS NUMBER(9, 2)) AS UNIT_PRICE,
        CAST(TRIM(src.W2AMT) AS NUMBER(11, 2)) AS EXTENDED_AMT,
        CAST(TRIM(src.W2CAMT) AS NUMBER(11, 2)) AS EXTENDED_COST,
        CAST(TRIM(src.W2GPAM) AS NUMBER(11, 2)) AS GP_AMT,
        CAST(TRIM(src.W2GPMG) AS NUMBER(5, 2)) AS GP_MARGIN,
        CAST(TRIM(src.W2GPNM) AS NUMBER(11, 2)) AS GP_NAB_AMT,
        CAST(TRIM(src.W2GPNG) AS NUMBER(5, 2)) AS GP_NAB_MARGIN,
        CAST(TRIM(src.W2MECH) AS NUMBER(10, 0)) AS MECHANIC_NUMBER,
        CAST(TRIM(src.W2IEXM) AS NUMBER(11, 2)) AS ITEM_TAX_EXEMPT_AMT,
        CAST(TRIM(src.W2ITXB) AS NUMBER(11, 2)) AS ITEM_TAXABLE_AMT,
        CAST(TRIM(src.W2ISLT) AS NUMBER(11, 2)) AS ITEM_SALES_TAX_AMT,
        CAST(TRIM(src.W2IAMT) AS NUMBER(11, 2)) AS TOTAL_ITEM_AMT,
        CAST(TRIM(src.W2CSTS) AS VARCHAR(1)) AS CHARGE_FLAG,
        CAST(TRIM(src.W2RSTS) AS VARCHAR(1)) AS ROLLUP_TO_PARENT_FLAG,
        CAST(TRIM(src.W2QITM) AS VARCHAR(1)) AS QUESTION_ITEM_FLAG,
        CAST(TRIM(src.W2PITM) AS NUMBER(10, 0)) AS PARENT_ITEM_NUMBER,
        CAST(TRIM(src.W2SFCD) AS NUMBER(3, 0)) AS SPIFF_CODE,
        CAST(TRIM(src.W2ISAM) AS NUMBER(9, 2)) AS ITEM_SPIFF_AMT,
        CAST(TRIM(src.W2ISPT) AS NUMBER(7, 0)) AS ITEM_SPIFF_POINTS,
        CAST(TRIM(src.W2IFEE) AS NUMBER(9, 2)) AS ITEM_TIRE_FEE,
        CAST(TRIM(src.W2OUPR) AS NUMBER(9, 2)) AS ORGIN_UNIT_PRICE,
        CAST(TRIM(src.W2QSTS) AS VARCHAR(1)) AS QUESTIONS_ASKED_FLAG,
        CAST(TRIM(src.W2NSTS) AS VARCHAR(1)) AS NOTES_ASKED_FLAG,
        CAST(TRIM(src.W2VEND) AS VARCHAR(20)) AS VENDOR_NUMBER,
        CAST(TRIM(src.W2VDNM) AS VARCHAR(20)) AS VENDOR_NAME,
        CAST(TRIM(src.W2INVC) AS VARCHAR(20)) AS INVOICE_NUMBER,
        CASE 
            WHEN src.W2INDT = 0 THEN NULL
            ELSE TRY_TO_DATE(src.W2INDT::varchar(8), 'YYYYMMDD')
        END AS INVOICE_DATE,
        CAST(TRIM(src.W2RSTK) AS VARCHAR(1)) AS RETURN_TO_STOCK_FLAG,
        CAST(TRIM(src.W2CTCD) AS NUMBER(3, 0)) AS INVENTORY_CATEGORY_CODE,
        CAST(TRIM(src.W2VSEQ) AS NUMBER(4, 0)) AS VEHICLE_SEQUENCE_NUMBER,
        CAST(TRIM(src.W2FDTX) AS NUMBER(9, 2)) AS FEDERAL_TAX,
        CAST(TRIM(src.W2AVCS) AS NUMBER(9, 2)) AS AVERAGE_COST,
        CAST(TRIM(src.W2NAVD) AS NUMBER(6, 0)) AS NAB_VENDOR_NUMBER,
        CAST(TRIM(src.W2NAPR) AS NUMBER(9, 2)) AS NAB_EACH_PRICE,
        CAST(TRIM(src.W2NADP) AS NUMBER(5, 2)) AS NAB_DEL_COM_PCT,
        CAST(TRIM(src.W2NADA) AS NUMBER(9, 2)) AS NAB_DEL_COM_1_AMT,
        CAST(TRIM(src.W2NAD2) AS NUMBER(9, 2)) AS NAB_DEL_COM_2_AMT,
        CAST(TRIM(src.W2NAD3) AS NUMBER(9, 2)) AS NAB_DEL_COM_3_AMT,
        CAST(TRIM(src.W2NAD4) AS NUMBER(9, 2)) AS NAB_DEL_COM_4_AMT,
        CAST(TRIM(src.W2NACD) AS NUMBER(9, 2)) AS NAB_CREDIT_DUE_AMT,
        CAST(TRIM(src.W2NACR) AS NUMBER(9, 2)) AS NAB_CREDIT_RECEIVED_AMT,
        CAST(TRIM(src.W2NACP) AS NUMBER(9, 2)) AS NAB_CREDIT_PENDING_AMT,
        CAST(TRIM(src.W2CAMTGP) AS NUMBER(11, 2)) AS EXTENDED_COST_GP_AMT,
        CAST(TRIM(src.W2AMTGP) AS NUMBER(11, 2)) AS EXTENDED_GP_AMT,
        CAST(TRIM(src.W2ACLN) AS VARCHAR(20)) AS ADJ_CLAIM_NUMBER,
        CAST(TRIM(src.W2GPDS) AS VARCHAR(1)) AS GROUP_DISCOUNT_ITEM_FLAG,
        
        -- ETL metadata columns
        CAST(TRIM(src.SOURCE_SYSTEM) AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST(TRIM(src.SOURCE_FILE_NAME) AS VARCHAR(100)) AS SOURCE_FILE_NAME,
        CAST(TRIM(src.BATCH_ID) AS VARCHAR(50)) AS BATCH_ID,
        CAST(TRIM(src.ETL_VERSION) AS VARCHAR(20)) AS ETL_VERSION,
        CAST(TRIM(src.OPERATION) AS VARCHAR(10)) AS OPERATION,
        TO_TIMESTAMP_NTZ(TRIM(src.ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_ITEM_WOMSTI') }} src
    {% if is_incremental() %}
    WHERE TO_TIMESTAMP_NTZ(TRIM(src.ENTRY_TIMESTAMP)) > (SELECT COALESCE(MAX(EFFECTIVE_DATE), '1899-12-31T00:00:00Z') FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(TRIM(src.W2STORE) AS NUMBER(3, 0)), 
                     CAST(TRIM(src.W2FMTP) AS VARCHAR(2)), 
                     CAST(TRIM(src.W2WIPX) AS VARCHAR(3)), 
                     CAST(TRIM(src.W2WO) AS NUMBER(10, 0)), 
                     CAST(TRIM(src.W2SEQ) AS NUMBER(4, 0))
        ORDER BY TO_TIMESTAMP_NTZ(TRIM(src.ENTRY_TIMESTAMP)) DESC,W2CYMD desc,W2HMS desc
    ) = 1
),

-- Step 2: Join with dimension tables and calculate hash
source_with_hash AS (
    SELECT
        src.*,
        dim.ORIGIN_TRANSACTION_SK,
        dim.WORK_ORDER_HEADER_SK,
        dp.PRODUCT_SK,
        mech.MECHANIC_SK,
        dp.INVENTORY_CATEGORY_SK,
        
        MD5(CONCAT_WS('|',
            COALESCE(CAST(src.PRODUCT_NUMBER AS VARCHAR), ''),
            COALESCE(src.MANUFACTURING_CODE, ''),
            COALESCE(CAST(src.QUANTITY AS VARCHAR), ''),
            COALESCE(src.SIZE, ''),
            COALESCE(src.DESCRIPTION, ''),
            COALESCE(src.PLY_RATING, ''),
            COALESCE(CAST(src.PRICE_CODE AS VARCHAR), ''),
            COALESCE(CAST(src.UNIT_COST AS VARCHAR), ''),
            COALESCE(CAST(src.UNIT_PRICE AS VARCHAR), ''),
            COALESCE(CAST(src.EXTENDED_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.EXTENDED_COST AS VARCHAR), ''),
            COALESCE(CAST(src.GP_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.GP_MARGIN AS VARCHAR), ''),
            COALESCE(CAST(src.GP_NAB_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.GP_NAB_MARGIN AS VARCHAR), ''),
            COALESCE(CAST(src.MECHANIC_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_TAX_EXEMPT_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_TAXABLE_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_SALES_TAX_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.TOTAL_ITEM_AMT AS VARCHAR), ''),
            COALESCE(src.CHARGE_FLAG, ''),
            COALESCE(src.ROLLUP_TO_PARENT_FLAG, ''),
            COALESCE(src.QUESTION_ITEM_FLAG, ''),
            COALESCE(CAST(src.PARENT_ITEM_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.SPIFF_CODE AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_SPIFF_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_SPIFF_POINTS AS VARCHAR), ''),
            COALESCE(CAST(src.ITEM_TIRE_FEE AS VARCHAR), ''),
            COALESCE(CAST(src.ORGIN_UNIT_PRICE AS VARCHAR), ''),
            COALESCE(src.QUESTIONS_ASKED_FLAG, ''),
            COALESCE(src.NOTES_ASKED_FLAG, ''),
            COALESCE(src.VENDOR_NUMBER, ''),
            COALESCE(src.VENDOR_NAME, ''),
            COALESCE(src.INVOICE_NUMBER, ''),
            COALESCE(CAST(src.INVOICE_DATE AS VARCHAR), ''),
            COALESCE(src.RETURN_TO_STOCK_FLAG, ''),
            COALESCE(CAST(src.INVENTORY_CATEGORY_CODE AS VARCHAR), ''),
            COALESCE(CAST(src.VEHICLE_SEQUENCE_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.FEDERAL_TAX AS VARCHAR), ''),
            COALESCE(CAST(src.AVERAGE_COST AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_VENDOR_NUMBER AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_EACH_PRICE AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_DEL_COM_PCT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_DEL_COM_1_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_DEL_COM_2_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_DEL_COM_3_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_DEL_COM_4_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_CREDIT_DUE_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_CREDIT_RECEIVED_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.NAB_CREDIT_PENDING_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.EXTENDED_COST_GP_AMT AS VARCHAR), ''),
            COALESCE(CAST(src.EXTENDED_GP_AMT AS VARCHAR), ''),
            COALESCE(src.ADJ_CLAIM_NUMBER, ''),
            COALESCE(src.GROUP_DISCOUNT_ITEM_FLAG, ''),
            -- Include join fields in hash
            COALESCE(CAST(dim.ORIGIN_TRANSACTION_SK AS VARCHAR), ''),
            COALESCE(CAST(dim.WORK_ORDER_HEADER_SK AS VARCHAR), ''),
            COALESCE(CAST(dp.PRODUCT_SK AS VARCHAR), ''),
            COALESCE(CAST(mech.MECHANIC_SK AS VARCHAR), ''),
            COALESCE(CAST(dp.INVENTORY_CATEGORY_SK AS VARCHAR), '')
        )) AS RECORD_CHECKSUM_HASH
    FROM source_data src
    INNER JOIN {{ ref('T_FCT_WORK_ORDER_HEADER') }} dim
      ON dim.STORE_NUMBER = src.STORE_NUMBER
     AND dim.FORM_TYPE_CODE = src.FORM_TYPE_CODE
     AND dim.POS_PREFIX = src.POS_PREFIX
     AND dim.WORK_ORDER_NUMBER = src.WORK_ORDER_NUMBER
     AND dim.IS_CURRENT_FLAG = TRUE
    LEFT JOIN {{ source('silver_data', 'T_DIM_PRODUCT') }} dp
      ON src.PRODUCT_NUMBER = dp.ITEM_NUMBER 
     AND src.ENTRY_TIMESTAMP BETWEEN dp.EFFECTIVE_DATE AND COALESCE(dp.EXPIRATION_DATE, '9999-12-31')
    LEFT JOIN {{ source('silver_data', 'T_DIM_MECHANIC') }} mech
      ON src.MECHANIC_NUMBER = mech.MECHANIC_ID 
     AND src.ENTRY_TIMESTAMP BETWEEN mech.EFFECTIVE_DATE AND COALESCE(mech.EXPIRATION_DATE, '9999-12-31')
),

-- Step 3: Identify changes (inserts/updates)
changes AS (
    SELECT *
    FROM source_with_hash
    WHERE OPERATION IN ('INSERT', 'UPDATE')
),

-- Step 4: Detect deletes
deletes AS (
    SELECT *
    FROM source_with_hash
    WHERE OPERATION = 'DELETE'
),

-- Step 5: Get max surrogate key
max_key AS (
    SELECT COALESCE(MAX(WORK_ORDER_ITEM_DETAILS_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 6: Generate new SCD2 rows for inserts/updates
new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER, oc.ITEM_SEQ_NUMBER, oc.ENTRY_TIMESTAMP
        ) + max_key.max_sk AS WORK_ORDER_ITEM_DETAILS_SK,
        
        oc.STORE_NUMBER,
        oc.FORM_TYPE_CODE,
        oc.POS_PREFIX,
        oc.WORK_ORDER_NUMBER,
        oc.ITEM_SEQ_NUMBER,
        oc.ORIGIN_TRANSACTION_SK,
        oc.WORK_ORDER_HEADER_SK,
        oc.PRODUCT_SK,
        oc.MANUFACTURING_CODE,
        oc.QUANTITY,
        oc.SIZE,
        oc.DESCRIPTION,
        oc.PLY_RATING,
        oc.PRICE_CODE,
        oc.UNIT_COST,
        oc.UNIT_PRICE,
        oc.EXTENDED_AMT,
        oc.EXTENDED_COST,
        oc.GP_AMT,
        oc.GP_MARGIN,
        oc.GP_NAB_AMT,
        oc.GP_NAB_MARGIN,
        oc.MECHANIC_SK,
        oc.ITEM_TAX_EXEMPT_AMT,
        oc.ITEM_TAXABLE_AMT,
        oc.ITEM_SALES_TAX_AMT,
        oc.TOTAL_ITEM_AMT,
        oc.CHARGE_FLAG,
        oc.ROLLUP_TO_PARENT_FLAG,
        oc.QUESTION_ITEM_FLAG,
        oc.PARENT_ITEM_NUMBER,
        oc.SPIFF_CODE,
        oc.ITEM_SPIFF_AMT,
        oc.ITEM_SPIFF_POINTS,
        oc.ITEM_TIRE_FEE,
        oc.ORGIN_UNIT_PRICE,
        oc.QUESTIONS_ASKED_FLAG,
        oc.NOTES_ASKED_FLAG,
        oc.VENDOR_NUMBER,
        oc.VENDOR_NAME,
        oc.INVOICE_NUMBER,
        oc.INVOICE_DATE,
        oc.RETURN_TO_STOCK_FLAG,
        oc.INVENTORY_CATEGORY_SK,
        oc.VEHICLE_SEQUENCE_NUMBER,
        oc.FEDERAL_TAX,
        oc.AVERAGE_COST,
        oc.NAB_VENDOR_NUMBER,
        oc.NAB_EACH_PRICE,
        oc.NAB_DEL_COM_PCT,
        oc.NAB_DEL_COM_1_AMT,
        oc.NAB_DEL_COM_2_AMT,
        oc.NAB_DEL_COM_3_AMT,
        oc.NAB_DEL_COM_4_AMT,
        oc.NAB_CREDIT_DUE_AMT,
        oc.NAB_CREDIT_RECEIVED_AMT,
        oc.NAB_CREDIT_PENDING_AMT,
        oc.EXTENDED_COST_GP_AMT,
        oc.EXTENDED_GP_AMT,
        oc.ADJ_CLAIM_NUMBER,
        oc.GROUP_DISCOUNT_ITEM_FLAG,
        
        -- SCD2 fields
        oc.ENTRY_TIMESTAMP AS EFFECTIVE_DATE,
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS EXPIRATION_DATE,
        TRUE AS IS_CURRENT_FLAG,
        
        oc.SOURCE_SYSTEM,
        oc.SOURCE_FILE_NAME,
        oc.BATCH_ID,
        oc.RECORD_CHECKSUM_HASH,
        oc.ETL_VERSION,
        CURRENT_TIMESTAMP AS INGESTION_DTTM,
        CURRENT_DATE AS INGESTION_DT
    FROM changes oc
    CROSS JOIN max_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} tgt
        WHERE CONCAT_WS('|', tgt.STORE_NUMBER, tgt.FORM_TYPE_CODE, tgt.POS_PREFIX, tgt.WORK_ORDER_NUMBER, tgt.ITEM_SEQ_NUMBER) = 
              CONCAT_WS('|', oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER, oc.ITEM_SEQ_NUMBER)
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),

-- Step 7: Expire old current rows when updates happen
expired_rows AS (
    SELECT
        old.WORK_ORDER_ITEM_DETAILS_SK,
        old.STORE_NUMBER,
        old.FORM_TYPE_CODE,
        old.POS_PREFIX,
        old.WORK_ORDER_NUMBER,
        old.ITEM_SEQ_NUMBER,
        old.ORIGIN_TRANSACTION_SK,
        old.WORK_ORDER_HEADER_SK,
        old.PRODUCT_SK,
        old.MANUFACTURING_CODE,
        old.QUANTITY,
        old.SIZE,
        old.DESCRIPTION,
        old.PLY_RATING,
        old.PRICE_CODE,
        old.UNIT_COST,
        old.UNIT_PRICE,
        old.EXTENDED_AMT,
        old.EXTENDED_COST,
        old.GP_AMT,
        old.GP_MARGIN,
        old.GP_NAB_AMT,
        old.GP_NAB_MARGIN,
        old.MECHANIC_SK,
        old.ITEM_TAX_EXEMPT_AMT,
        old.ITEM_TAXABLE_AMT,
        old.ITEM_SALES_TAX_AMT,
        old.TOTAL_ITEM_AMT,
        old.CHARGE_FLAG,
        old.ROLLUP_TO_PARENT_FLAG,
        old.QUESTION_ITEM_FLAG,
        old.PARENT_ITEM_NUMBER,
        old.SPIFF_CODE,
        old.ITEM_SPIFF_AMT,
        old.ITEM_SPIFF_POINTS,
        old.ITEM_TIRE_FEE,
        old.ORGIN_UNIT_PRICE,
        old.QUESTIONS_ASKED_FLAG,
        old.NOTES_ASKED_FLAG,
        old.VENDOR_NUMBER,
        old.VENDOR_NAME,
        old.INVOICE_NUMBER,
        old.INVOICE_DATE,
        old.RETURN_TO_STOCK_FLAG,
        old.INVENTORY_CATEGORY_SK,
        old.VEHICLE_SEQUENCE_NUMBER,
        old.FEDERAL_TAX,
        old.AVERAGE_COST,
        old.NAB_VENDOR_NUMBER,
        old.NAB_EACH_PRICE,
        old.NAB_DEL_COM_PCT,
        old.NAB_DEL_COM_1_AMT,
        old.NAB_DEL_COM_2_AMT,
        old.NAB_DEL_COM_3_AMT,
        old.NAB_DEL_COM_4_AMT,
        old.NAB_CREDIT_DUE_AMT,
        old.NAB_CREDIT_RECEIVED_AMT,
        old.NAB_CREDIT_PENDING_AMT,
        old.EXTENDED_COST_GP_AMT,
        old.EXTENDED_GP_AMT,
        old.ADJ_CLAIM_NUMBER,
        old.GROUP_DISCOUNT_ITEM_FLAG,
        
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
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER, old.ITEM_SEQ_NUMBER) = 
         CONCAT_WS('|', new.STORE_NUMBER, new.FORM_TYPE_CODE, new.POS_PREFIX, new.WORK_ORDER_NUMBER, new.ITEM_SEQ_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 8: Soft deletes - expire records if source says DELETE
soft_deletes AS (
    SELECT
        old.WORK_ORDER_ITEM_DETAILS_SK,
        old.STORE_NUMBER,
        old.FORM_TYPE_CODE,
        old.POS_PREFIX,
        old.WORK_ORDER_NUMBER,
        old.ITEM_SEQ_NUMBER,
        old.ORIGIN_TRANSACTION_SK,
        old.WORK_ORDER_HEADER_SK,
        old.PRODUCT_SK,
        old.MANUFACTURING_CODE,
        old.QUANTITY,
        old.SIZE,
        old.DESCRIPTION,
        old.PLY_RATING,
        old.PRICE_CODE,
        old.UNIT_COST,
        old.UNIT_PRICE,
        old.EXTENDED_AMT,
        old.EXTENDED_COST,
        old.GP_AMT,
        old.GP_MARGIN,
        old.GP_NAB_AMT,
        old.GP_NAB_MARGIN,
        old.MECHANIC_SK,
        old.ITEM_TAX_EXEMPT_AMT,
        old.ITEM_TAXABLE_AMT,
        old.ITEM_SALES_TAX_AMT,
        old.TOTAL_ITEM_AMT,
        old.CHARGE_FLAG,
        old.ROLLUP_TO_PARENT_FLAG,
        old.QUESTION_ITEM_FLAG,
        old.PARENT_ITEM_NUMBER,
        old.SPIFF_CODE,
        old.ITEM_SPIFF_AMT,
        old.ITEM_SPIFF_POINTS,
        old.ITEM_TIRE_FEE,
        old.ORGIN_UNIT_PRICE,
        old.QUESTIONS_ASKED_FLAG,
        old.NOTES_ASKED_FLAG,
        old.VENDOR_NUMBER,
        old.VENDOR_NAME,
        old.INVOICE_NUMBER,
        old.INVOICE_DATE,
        old.RETURN_TO_STOCK_FLAG,
        old.INVENTORY_CATEGORY_SK,
        old.VEHICLE_SEQUENCE_NUMBER,
        old.FEDERAL_TAX,
        old.AVERAGE_COST,
        old.NAB_VENDOR_NUMBER,
        old.NAB_EACH_PRICE,
        old.NAB_DEL_COM_PCT,
        old.NAB_DEL_COM_1_AMT,
        old.NAB_DEL_COM_2_AMT,
        old.NAB_DEL_COM_3_AMT,
        old.NAB_DEL_COM_4_AMT,
        old.NAB_CREDIT_DUE_AMT,
        old.NAB_CREDIT_RECEIVED_AMT,
        old.NAB_CREDIT_PENDING_AMT,
        old.EXTENDED_COST_GP_AMT,
        old.EXTENDED_GP_AMT,
        old.ADJ_CLAIM_NUMBER,
        old.GROUP_DISCOUNT_ITEM_FLAG,
        
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
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER, old.ITEM_SEQ_NUMBER) = 
         CONCAT_WS('|', del.STORE_NUMBER, del.FORM_TYPE_CODE, del.POS_PREFIX, del.WORK_ORDER_NUMBER, del.ITEM_SEQ_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
)

-- Final output
SELECT * FROM expired_rows
UNION ALL
SELECT * FROM soft_deletes
UNION ALL
SELECT * FROM new_rows