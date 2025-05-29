{{ config(
    materialized='incremental',
    unique_key='WORK_ORDER_ITEM_DETAILS_SK'
) }}

-- Step 1: Load source data with filtering on ENTRY_TIMESTAMP for incremental runs
WITH max_loaded AS (
    SELECT COALESCE(MAX(ENTRY_TIMESTAMP), TO_TIMESTAMP_NTZ('1900-01-01')) AS max_ts FROM  {{ source('bronze_data', 'T_BRZ_ITEM_WOMSTI') }}
),

source_data AS (
 SELECT
    CAST(TRIM(W2STORE) AS NUMBER(3, 0)) AS STORE_NUMBER,
    CAST(TRIM(W2FMTP) AS VARCHAR(2)) AS FORM_TYPE_CODE,
    CAST(TRIM(W2WIPX) AS VARCHAR(3)) AS POS_PREFIX,
    CAST(TRIM(W2WO) AS NUMBER(10, 0)) AS WORK_ORDER_NUMBER,
    CAST(TRIM(W2SEQ) AS NUMBER(4, 0)) AS ITEM_SEQ_NUMBER,
    CAST(TRIM(W2ITM) AS NUMBER(10, 0)) AS PRODUCT_SK,
    CAST(TRIM(W2MFCD) AS VARCHAR(20)) AS MANUFACTURING_CODE,

    CAST(TRIM(W2QTY) AS NUMBER(7, 0)) AS QUANTITY,
    CAST(TRIM(W2SIZE) AS VARCHAR(20)) AS SIZE,
    CAST(TRIM(W2DESC) AS VARCHAR(40)) AS DESCRIPTION,
    CAST(TRIM(W2PLY) AS VARCHAR(5)) AS PLY_RATING,
    CAST(TRIM(W2PRCD) AS NUMBER(3, 0)) AS PRICE_CODE,
    CAST(TRIM(W2UNCS) AS NUMBER(9, 2)) AS UNIT_COST,
    CAST(TRIM(W2UNPR) AS NUMBER(9, 2)) AS UNIT_PRICE,
    CAST(TRIM(W2AMT) AS NUMBER(11, 2)) AS EXTENDED_AMT,
    CAST(TRIM(W2CAMT) AS NUMBER(11, 2)) AS EXTENDED_COST,
    CAST(TRIM(W2GPAM) AS NUMBER(11, 2)) AS GP_AMT,
    CAST(TRIM(W2GPMG) AS NUMBER(5, 2)) AS GP_MARGIN,
    CAST(TRIM(W2GPNM) AS NUMBER(11, 2)) AS GP_NAB_AMT,
    CAST(TRIM(W2GPNG) AS NUMBER(5, 2)) AS GP_NAB_MARGIN,
    CAST(TRIM(W2MECH) AS NUMBER(10, 0)) AS MECHANIC_SK,
    CAST(TRIM(W2IEXM) AS NUMBER(11, 2)) AS ITEM_TAX_EXEMPT_AMT,
    CAST(TRIM(W2ITXB) AS NUMBER(11, 2)) AS ITEM_TAXABLE_AMT,
    CAST(TRIM(W2ISLT) AS NUMBER(11, 2)) AS ITEM_SALES_TAX_AMT,
    CAST(TRIM(W2IAMT) AS NUMBER(11, 2)) AS TOTAL_ITEM_AMT,
    CAST(TRIM(W2CSTS) AS VARCHAR(1)) AS CHARGE_FLAG,
    CAST(TRIM(W2RSTS) AS VARCHAR(1)) AS ROLLUP_TO_PARENT_FLAG,
    CAST(TRIM(W2QITM) AS VARCHAR(1)) AS QUESTION_ITEM_FLAG,
    CAST(TRIM(W2PITM) AS NUMBER(10, 0)) AS PARENT_ITEM_NUMBER,
    CAST(TRIM(W2SFCD) AS NUMBER(3, 0)) AS SPIFF_CODE,
    CAST(TRIM(W2ISAM) AS NUMBER(9, 2)) AS ITEM_SPIFF_AMT,
    CAST(TRIM(W2ISPT) AS NUMBER(7, 0)) AS ITEM_SPIFF_POINTS,
    CAST(TRIM(W2IFEE) AS NUMBER(9, 2)) AS ITEM_TIRE_FEE,
    CAST(TRIM(W2OUPR) AS NUMBER(9, 2)) AS ORGIN_UNIT_PRICE,
    CAST(TRIM(W2QSTS) AS VARCHAR(1)) AS QUESTIONS_ASKED_FLAG,
    CAST(TRIM(W2NSTS) AS VARCHAR(1)) AS NOTES_ASKED_FLAG,
    CAST(TRIM(W2VEND) AS VARCHAR(20)) AS VENDOR_NUMBER,
    CAST(TRIM(W2VDNM) AS VARCHAR(20)) AS VENDOR_NAME,
    CAST(TRIM(W2INVC) AS VARCHAR(20)) AS INVOICE_NUMBER,
    CASE 
        WHEN W2INDT = 0 THEN NULL
        ELSE try_TO_DATE(W2INDT::varchar(8), 'YYYYMMDD')
    END AS INVOICE_DATE,
    CAST(TRIM(W2RSTK) AS VARCHAR(1)) AS RETURN_TO_STOCK_FLAG,
    CAST(TRIM(W2CTCD) AS NUMBER(3, 0)) AS INVENTORY_CATEGORY_SK,
    CAST(TRIM(W2VSEQ) AS NUMBER(4, 0)) AS VEHICLE_SEQUENCE_NUMBER,
    CAST(TRIM(W2FDTX) AS NUMBER(9, 2)) AS FEDERAL_TAX,
    CAST(TRIM(W2AVCS) AS NUMBER(9, 2)) AS AVERAGE_COST,
    CAST(TRIM(W2NAVD) AS NUMBER(6, 0)) AS NAB_VENDOR_NUMBER,
    CAST(TRIM(W2NAPR) AS NUMBER(9, 2)) AS NAB_EACH_PRICE,
    CAST(TRIM(W2NADP) AS NUMBER(5, 2)) AS NAB_DEL_COM_PCT,
    CAST(TRIM(W2NADA) AS NUMBER(9, 2)) AS NAB_DEL_COM_1_AMT,
    CAST(TRIM(W2NAD2) AS NUMBER(9, 2)) AS NAB_DEL_COM_2_AMT,
    CAST(TRIM(W2NAD3) AS NUMBER(9, 2)) AS NAB_DEL_COM_3_AMT,
    CAST(TRIM(W2NAD4) AS NUMBER(9, 2)) AS NAB_DEL_COM_4_AMT,
    CAST(TRIM(W2NACD) AS NUMBER(9, 2)) AS NAB_CREDIT_DUE_AMT,
    CAST(TRIM(W2NACR) AS NUMBER(9, 2)) AS NAB_CREDIT_RECEIVED_AMT,
    CAST(TRIM(W2NACP) AS NUMBER(9, 2)) AS NAB_CREDIT_PENDING_AMT,
    CAST(TRIM(W2CAMTGP) AS NUMBER(11, 2)) AS EXTENDED_COST_GP_AMT,
    CAST(TRIM(W2AMTGP) AS NUMBER(11, 2)) AS EXTENDED_GP_AMT,
    CAST(TRIM(W2ACLN) AS VARCHAR(20)) AS ADJ_CLAIM_NUMBER,
    CAST(TRIM(W2GPDS) AS VARCHAR(1)) AS GROUP_DISCOUNT_ITEM_FLAG,

    -- ETL metadata columns
    CURRENT_TIMESTAMP() AS EFFECTIVE_DATE,
    TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS EXPIRATION_DATE,
    TRUE AS IS_CURRENT_FLAG,

    CAST(TRIM(SOURCE_SYSTEM) AS VARCHAR(50)) AS SOURCE_SYSTEM,
    CAST(TRIM(SOURCE_FILE_NAME) AS VARCHAR(100)) AS SOURCE_FILE_NAME,
    CAST(TRIM(BATCH_ID) AS VARCHAR(50)) AS BATCH_ID,
    CAST(TRIM(ETL_VERSION) AS VARCHAR(20)) AS ETL_VERSION,
    CAST(TRIM(OPERATION) AS VARCHAR(10)) AS OPERATION,

        MD5(CONCAT(
            COALESCE(TRIM(W2ITM), ''),
            COALESCE(TRIM(W2MFCD), ''),
            COALESCE(TRIM(W2QTY), ''),
            COALESCE(TRIM(W2SIZE), ''),
            COALESCE(TRIM(W2DESC), ''),
            COALESCE(TRIM(W2PLY), ''),
            COALESCE(TRIM(W2PRCD), ''),
            COALESCE(TRIM(W2UNCS), ''),
            COALESCE(TRIM(W2UNPR), ''),
            COALESCE(TRIM(W2AMT), ''),
            COALESCE(TRIM(W2CAMT), ''),
            COALESCE(TRIM(W2GPAM), ''),
            COALESCE(TRIM(W2GPMG), ''),
            COALESCE(TRIM(W2GPNM), ''),
            COALESCE(TRIM(W2GPNG), ''),
            COALESCE(TRIM(W2MECH), ''),
            COALESCE(TRIM(W2IEXM), ''),
            COALESCE(TRIM(W2ITXB), ''),
            COALESCE(TRIM(W2ISLT), ''),
            COALESCE(TRIM(W2IAMT), ''),
            COALESCE(TRIM(W2CSTS), ''),
            COALESCE(TRIM(W2RSTS), ''),
            COALESCE(TRIM(W2QITM), ''),
            COALESCE(TRIM(W2PITM), ''),
            COALESCE(TRIM(W2SFCD), ''),
            COALESCE(TRIM(W2ISAM), ''),
            COALESCE(TRIM(W2ISPT), ''),
            COALESCE(TRIM(W2IFEE), ''),
            COALESCE(TRIM(W2OUPR), ''),
            COALESCE(TRIM(W2QSTS), ''),
            COALESCE(TRIM(W2NSTS), ''),
            COALESCE(TRIM(W2VEND), ''),
            COALESCE(TRIM(W2VDNM), ''),
            COALESCE(TRIM(W2INVC), ''),
            COALESCE(TRIM(W2INDT), ''),
            COALESCE(TRIM(W2RSTK), ''),
            COALESCE(TRIM(W2CTCD), ''),
            COALESCE(TRIM(W2VSEQ), ''),
            COALESCE(TRIM(W2FDTX), ''),
            COALESCE(TRIM(W2AVCS), ''),
            COALESCE(TRIM(W2NAVD), ''),
            COALESCE(TRIM(W2NAPR), ''),
            COALESCE(TRIM(W2NADP), ''),
            COALESCE(TRIM(W2NADA), ''),
            COALESCE(TRIM(W2NAD2), ''),
            COALESCE(TRIM(W2NAD3), ''),
            COALESCE(TRIM(W2NAD4), ''),
            COALESCE(TRIM(W2NACD), ''),
            COALESCE(TRIM(W2NACR), ''),
            COALESCE(TRIM(W2NACP), ''),
            COALESCE(TRIM(W2CAMTGP), ''),
            COALESCE(TRIM(W2AMTGP), ''),
            COALESCE(TRIM(W2ACLN), ''),
            COALESCE(TRIM(W2GPDS), '')
        )) AS RECORD_CHECKSUM_HASH,
        CURRENT_TIMESTAMP() AS INGESTION_DTTM,
        CURRENT_DATE() AS INGESTION_DT,
        TO_TIMESTAMP_NTZ(TRIM(ENTRY_TIMESTAMP)) AS ENTRY_TIMESTAMP
    FROM {{ source('bronze_data', 'T_BRZ_ITEM_WOMSTI') }}
    --    {% if is_incremental() %}
    -- WHERE ENTRY_TIMESTAMP ='1900-01-01T00:00:00Z'
    --     {% endif %}
),

-- Step 2: Join with dimension tables and generate surrogate key as hash of natural keys
source_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY  sd.STORE_NUMBER,sd.FORM_TYPE_CODE,sd.POS_PREFIX ,sd.WORK_ORDER_NUMBER,sd.ITEM_SEQ_NUMBER
        ) AS WORK_ORDER_ITEM_DETAILS_SK,
        sd.*,
        -- invv.INVENTORY_VENDOR_SK as INVENTORY_VENDOR_SK,
        dim.ORIGIN_TRANSACTION_SK as ORIGIN_TRANSACTION_SK,
        dim.WORK_ORDER_HEADER_SK as WORK_ORDER_HEADER_SK,
        dp.PRODUCT_SK AS DIM_PRODUCT_SK,
        mech.MECHANIC_SK as DIM_MECHANIC_SK,
        ic.INVENTORY_CATEGORY_SK AS DIM_INVENTORY_CATEGORY_SK
    FROM source_data sd
    INNER JOIN {{ ref('T_FCT_WORK_ORDER_HEADER') }} dim
      ON dim.STORE_NUMBER = sd.STORE_NUMBER
      and dim.FORM_TYPE_CODE = sd.FORM_TYPE_CODE
      and dim.POS_PREFIX = sd.POS_PREFIX    
      and dim.WORK_ORDER_NUMBER = sd.WORK_ORDER_NUMBER
     AND dim.IS_CURRENT_FLAG = TRUE
    LEFT JOIN  {{ source('silver_data', 'T_DIM_PRODUCT') }} dp
        ON sd.PRODUCT_SK = dp.PRODUCT_SK AND dp.IS_CURRENT_FLAG = TRUE
    LEFT JOIN {{ source('silver_data', 'T_DIM_MECHANIC') }} mech
        ON sd.INVENTORY_CATEGORY_SK = mech.MECHANIC_SK AND mech.IS_CURRENT_FLAG = TRUE 
    --      LEFT JOIN {{ ref('T_DIM_INVENTORY_VENDOR') }} invv
    --   ON invv.VENDOR_NUMBER = sd.VENDOR_NUMBER
    --  AND sd.ENTRY_TIMESTAMP BETWEEN invv.EFFECTIVE_DATE AND COALESCE(invv.EXPIRATION_DATE, '9999-12-31')  
    LEFT JOIN {{ source('silver_data', 'T_DIM_INVENTORY_CATEGORY') }} ic
        ON sd.INVENTORY_CATEGORY_SK = ic.INVENTORY_CATEGORY_SK AND ic.IS_CURRENT_FLAG = TRUE
        
    
),

-- -- Final output
-- SELECT *
-- FROM source_with_keys

-- Step 3: Rank records by business key to handle duplicates
ranked_source AS (
    SELECT 
        swk.*,
        ROW_NUMBER() OVER (
            PARTITION BY CONCAT_WS('|', swk.STORE_NUMBER, swk.FORM_TYPE_CODE, swk.POS_PREFIX, swk.WORK_ORDER_NUMBER,swk.ITEM_SEQ_NUMBER)
            ORDER BY swk.ENTRY_TIMESTAMP DESC
        ) AS rn
    FROM source_with_keys swk
),

-- Step 4: Remove duplicates by taking the latest record for each business key
deduplicated_source AS (
    SELECT * FROM ranked_source WHERE rn = 1
),

-- Step 5: Add previous hash for change detection
source_with_lag AS (
    SELECT
        curr.*,
        LAG(RECORD_CHECKSUM_HASH) OVER (
            PARTITION BY CONCAT_WS('|', curr.STORE_NUMBER, curr.FORM_TYPE_CODE, curr.POS_PREFIX, curr.WORK_ORDER_NUMBER,curr.ITEM_SEQ_NUMBER) 
            ORDER BY curr.ENTRY_TIMESTAMP
        ) AS prev_hash
    FROM deduplicated_source curr
),

-- Step 6: Identify changes (new records or records with changed hash)
changes AS (
    SELECT *
    FROM source_with_lag
    WHERE (RECORD_CHECKSUM_HASH != prev_hash OR prev_hash IS NULL)
      AND OPERATION != 'DELETE'
),

-- Step 7: Identify deleted records
deletes AS (
    SELECT *
    FROM deduplicated_source
    WHERE OPERATION = 'DELETE'
),

-- Step 8: Get the maximum existing surrogate key
max_key AS (
    SELECT COALESCE(MAX(WORK_ORDER_ITEM_DETAILS_SK), 0) AS max_sk FROM {{ this }}
),

-- Step 9: Order changes and prepare for surrogate key assignment
ordered_changes AS (
    SELECT 
        ch.*,
        LEAD(ENTRY_TIMESTAMP) OVER (
            PARTITION BY CONCAT_WS('|', ch.STORE_NUMBER, ch.FORM_TYPE_CODE, ch.POS_PREFIX, ch.WORK_ORDER_NUMBER,ch.ITEM_SEQ_NUMBER) 
            ORDER BY ENTRY_TIMESTAMP
        ) AS next_entry_ts
    FROM changes ch
),

new_rows AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY CONCAT_WS('|', oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER,oc.ITEM_SEQ_NUMBER), oc.ENTRY_TIMESTAMP
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
        -- oc.INVENTORY_VENDOR_SK,
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
        

        -- Effective/Expiration date and current flag logic
         TRY_TO_DATE(oc.ORIGIN_TRANSACTION_SK::varchar(8),'YYYYMMDD') AS EFFECTIVE_DATE,
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
        FROM ANALYTICS.SILVER_SALES.T_FCT_WORK_ORDER_ITEM_DETAILS tgt
        WHERE CONCAT_WS('|', tgt.STORE_NUMBER, tgt.FORM_TYPE_CODE, tgt.POS_PREFIX, tgt.WORK_ORDER_NUMBER) = 
              CONCAT_WS('|', oc.STORE_NUMBER, oc.FORM_TYPE_CODE, oc.POS_PREFIX, oc.WORK_ORDER_NUMBER)
          AND tgt.EFFECTIVE_DATE = oc.ENTRY_TIMESTAMP
          AND tgt.RECORD_CHECKSUM_HASH = oc.RECORD_CHECKSUM_HASH
          AND tgt.IS_CURRENT_FLAG = TRUE
    )
),
-- Step 11: Identify records to expire (current records that have changed)
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
        -- old.INVENTORY_VENDOR_SK,
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
    FROM ANALYTICS.SILVER_SALES.T_FCT_WORK_ORDER_ITEM_DETAILS old
    JOIN new_rows new
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER) = 
         CONCAT_WS('|', new.STORE_NUMBER, new.FORM_TYPE_CODE, new.POS_PREFIX, new.WORK_ORDER_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
     AND old.RECORD_CHECKSUM_HASH != new.RECORD_CHECKSUM_HASH
),

-- Step 12: Identify records to soft delete

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
        -- old.INVENTORY_VENDOR_SK,
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
    FROM ANALYTICS.SILVER_SALES.T_FCT_WORK_ORDER_ITEM_DETAILS old
    JOIN deletes del
      ON CONCAT_WS('|', old.STORE_NUMBER, old.FORM_TYPE_CODE, old.POS_PREFIX, old.WORK_ORDER_NUMBER) = 
         CONCAT_WS('|', del.STORE_NUMBER, del.FORM_TYPE_CODE, del.POS_PREFIX, del.WORK_ORDER_NUMBER)
     AND old.IS_CURRENT_FLAG = TRUE
)

select * from new_rows
UNION ALL
select * from expired_rows
UNION ALL
select * FROM soft_deletes