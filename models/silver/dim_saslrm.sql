    SELECT
        ENTRY_TIMESTAMP,
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
        md5(
            coalesce(M0SLRP, '') || '|' ||
            coalesce(M0NAME, '') || '|' ||
            coalesce(M0SPCM, '')
        ) AS row_hash,
        ENTRY_TIMESTAMP AS EFFECTIVE_START_DATE,
        NULL AS EFFECTIVE_END_DATE,
        TRUE AS CURRENT_FLAG
    FROM {{ source('raw_data', 'T_BRZ_SALESREP_MASTER_SASLRM') }}
