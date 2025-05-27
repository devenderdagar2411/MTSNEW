{{ config(
    materialized = 'incremental',
    schema = 'SILVER_SALES',
    alias = 'T_DIM_ITEM_ADD_ON',
    unique_key = 'ITEM_NUMBER'
) }}

with source_data as (
    select
        ENTRY_TIMESTAMP,
        SEQUENCE_NUMBER,
        OPERATION,
        CAST(TRIM("I00ITM#") AS NUMERIC(10,0)) as "I00ITM#",
        CAST(TRIM(I00LADT) AS NUMERIC(8,0)) AS I00LADT,
        CAST(TRIM(I00BSCD) AS NUMERIC(3,0)) AS I00BSCD,
        CAST(TRIM(I00BRCD) AS NUMERIC(3,0)) AS I00BRCD,
        CAST(TRIM(I00NAMP) AS NUMERIC(9,2)) AS I00NAMP,
        CAST(TRIM(I00BCRP) AS VARCHAR(1)) AS I00BCRP,
        CAST(TRIM(I00BCLT) AS VARCHAR(1)) AS I00BCLT,
        CAST(TRIM(I00EX6600) AS NUMERIC(8,0)) AS I00EX6600,
        CAST(TRIM("I00FS#") AS VARCHAR(7)) AS "I00FS#",
        CAST(TRIM(I00FSMAXP) AS NUMERIC(9,2)) AS I00FSMAXP,
        CAST(TRIM("I00GV#") AS VARCHAR(8)) AS "I00GV#",
        CAST(TRIM(I00GVMAXP) AS NUMERIC(9,2)) AS I00GVMAXP,
        CAST(TRIM("I00OT#") AS VARCHAR(8)) AS "I00OT#",
        CAST(TRIM(I00OTMAXP) AS NUMERIC(9,2)) AS I00OTMAXP,
        CAST(TRIM(I00NU01) AS VARCHAR(40)) AS I00NU01,
        CAST(TRIM(I00NU02) AS VARCHAR(20)) AS I00NU02,
        CAST(TRIM(I00NU03) AS VARCHAR(30)) AS I00NU03,
        CAST(TRIM(I00NU04) AS VARCHAR(10)) AS I00NU04,
        CAST(TRIM(I00NU05) AS VARCHAR(1))  AS I00NU05,
        CAST(TRIM(I00NU06) AS VARCHAR(40)) AS I00NU06,
        CAST(TRIM(I00NU07) AS VARCHAR(20)) AS I00NU07,
        CAST(TRIM(I00NU08) AS VARCHAR(30)) AS I00NU08,
        CAST(TRIM(I00NU09) AS VARCHAR(10)) AS I00NU09,
        CAST(TRIM(I00NU10) AS VARCHAR(1))  AS I00NU10,
        CAST(TRIM(I00NU11) AS NUMERIC(15,6)) AS I00NU11,
        CAST(TRIM(I00NU12) AS NUMERIC(15,6)) AS I00NU12,
        CAST(TRIM(I00NU13) AS NUMERIC(15,6)) AS I00NU13,
        CAST(TRIM(I00NU14) AS NUMERIC(15,6)) AS I00NU14,
        CAST(TRIM(I00NU15) AS NUMERIC(15,6)) AS I00NU15,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        BATCH_ID,
        ETL_VERSION,
        INGESTION_DTTM,
        INGESTION_DT
    FROM {{ source('bronze_data', 'T_BRZ_ITEM_ADD_ON_INITEMAO') }}

    {% if is_incremental() %}
    WHERE INGESTION_DTTM > (
        SELECT COALESCE(MAX(INGESTION_DTTM), '1900-01-01') FROM {{ this }}
    )
    {% endif %}
),

ranked_data as (
    select
        *,
        row_number() over (
            partition by "I00ITM#"
            order by ENTRY_TIMESTAMP desc
        ) as rn
    from source_data
),

final_data as (
    select
        *,
        MD5(CONCAT_WS('|',
         COALESCE(TO_VARCHAR(I00LADT), ''),
            COALESCE(TO_VARCHAR(I00BSCD), ''),
            COALESCE(TO_VARCHAR(I00BRCD), ''),
            COALESCE(TO_VARCHAR(I00NAMP), ''),
            COALESCE(I00BCRP, ''),
            COALESCE(I00BCLT, ''),
            COALESCE(TO_VARCHAR(I00EX6600), ''),
            COALESCE("I00FS#", ''),
            COALESCE(TO_VARCHAR(I00FSMAXP), ''),
            COALESCE("I00GV#", ''),
            COALESCE(TO_VARCHAR(I00GVMAXP), ''),
            COALESCE("I00OT#", ''),
            COALESCE(TO_VARCHAR(I00OTMAXP), ''),
            COALESCE(I00NU01, ''),
            COALESCE(I00NU02, ''),
            COALESCE(I00NU03, ''),
            COALESCE(I00NU04, ''),
            COALESCE(I00NU05, ''),
            COALESCE(I00NU06, ''),
            COALESCE(I00NU07, ''),
            COALESCE(I00NU08, ''),
            COALESCE(I00NU09, ''),
            COALESCE(I00NU10, ''),
            COALESCE(TO_VARCHAR(I00NU11), ''),
            COALESCE(TO_VARCHAR(I00NU12), ''),
            COALESCE(TO_VARCHAR(I00NU13), ''),
            COALESCE(TO_VARCHAR(I00NU14), ''),
            COALESCE(TO_VARCHAR(I00NU15), '')
        )) AS RECORD_CHECKSUM_HASH,
        ABS(HASH("I00ITM#" || '|' || I00NAMP)) as ITEM_NUMBER_KEY
    from ranked_data
    where rn = 1
)

select
    CAST(ITEM_NUMBER_KEY AS NUMBER(20,0)) AS ITEM_NUMBER_KEY,
    "I00ITM#"        AS ITEM_NUMBER,
    I00LADT          AS LAST_ACTIVITY,
    I00BSCD          AS BS_OTR_CLASS,
    I00BRCD          AS BRAND_CODE,
    I00NAMP          AS NON_ADRS_MAX_PRICE,
    I00BCRP          AS BARCODE,
    I00BCLT          AS GENERATE_LOT_BARCODE,
    I00EX6600        AS EXP_DT,
    "I00FS#"         AS FIRESTONE_NUMBER,
    I00FSMAXP        AS FS_MAX_PRICE,
    "I00GV#"         AS GOVT_NUMBER,
    I00GVMAXP        AS GVT_MAX_PRICE,
    "I00OT#"         AS OTR_NUMBER,
    I00OTMAXP        AS OTR_MAX_PRICE,
    I00NU01          AS NOT_USED_01,
    I00NU02          AS NOT_USED_02,
    I00NU03          AS NOT_USED_03,
    I00NU04          AS NOT_USED_04,
    I00NU05          AS NO_CHARGE_ITEM,
    I00NU06          AS NOT_USED_06,
    I00NU07          AS NOT_USED_07,
    I00NU08          AS NOT_USED_08,
    I00NU09          AS NOT_USED_09,
    I00NU10          AS NOT_USED_10,
    I00NU11          AS NOT_USED_11,
    I00NU12          AS NOT_USED_12,
    I00NU13          AS NOT_USED_13,
    I00NU14          AS NOT_USED_14,
    I00NU15          AS NOT_USED_15,
    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    BATCH_ID,
    RECORD_CHECKSUM_HASH,
    ETL_VERSION,
    INGESTION_DTTM,
    INGESTION_DT
from final_data
