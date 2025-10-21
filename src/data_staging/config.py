#username and password
user = "blueinfo_os_2"
password = "UrI0avb0ds0vnJ4rz4E1"
endpoint_url = 'http://access.icos.datalake.vnpt.vn:80'
aws_access_key_id = '47yFLHe5c6Vy9mJaF9fQ'
aws_secret_access_key = 'TZBxi4oQLmeo1nXyWkfwIGORM4RIuDSMaZntykuS'

# path for quality check
data_quality_log_path = "cos://media_os_blueinfo_dev.datalake/logs/data_quality.parquet"
raw_log_path = "cos://media_os_blueinfo_dev.datalake/logs/staging_logs.parquet"

# path for staging
staging_log_path = "cos://media_os_blueinfo_dev.datalake/logs/staging_logs.parquet"  
data_path = "cos://media_os_blueinfo_dev.datalake/raw/"
data_layer = 'raw_data'

# table config
table_config = {
    'blueinfo_ccbs_bangphieutra': {'freq': 'monthly', 'partition_col': 'MA_KH'},
    'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb': {'freq': 'daily', 'partition_col': 'somay'},
    'blueinfo_ccbs_ct_no': {'freq': 'monthly', 'partition_col': 'MA_TB'},
    'blueinfo_ccbs_ct_tra': {'freq': 'monthly', 'partition_col': 'MA_TB'},
    'blueinfo_ccbs_cv207': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ccbs_cv207_cumulative': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ccbs_nops': {'freq': 'monthly', 'partition_col': 'MA_TB'},
    'blueinfo_ccbs_spi_3g_subs': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ccbs_thdd': {'freq': 'monthly', 'partition_col': 'MA_TB'},
    'blueinfo_dbvnp_prepaid_subscribers_history': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ggsn': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_ocs_air': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ocs_crs_disconnected_subscriber': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_ocs_crs_usage': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_ocs_crs_usageda': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_ocs_sdp_dedicatedaccount': {'freq': 'daily', 'partition_col': 'account_id'},
    'blueinfo_ocs_sdp_subscriber': {'freq': 'daily', 'partition_col': 'account_id'},
    'blueinfo_rims_celllac': {'freq': 'daily', 'partition_col': 'CELLL_CODE'},
    'blueinfo_smrs_dwd_geo_rvn_mtd': {'freq': 'daily', 'partition_col': 'ACCT_KEY'},
    'blueinfo_tac_gsma': {'freq': 'daily', 'partition_col': 'TAC'},
    'blueinfo_vascdr_2friend_log': {'freq': 'daily', 'partition_col': 'SENDER'},
    'blueinfo_vascdr_brandname_meta': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_vascdr_udv_credit_log': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_vascdr_utn_credit_log': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_vascloud_da': {'freq': 'daily', 'partition_col': 'msisdn'},
    'blueinfo_voice_msc': {'freq': 'daily', 'partition_col': 'MSISDN'},
    'blueinfo_voice_volte': {'freq': 'daily', 'partition_col': 'a_subs'}
}

# config for data quality
quality_conf = {
    "row_count": {
        "raw_data": {
            "blueinfo_ccbs_bangphieutra": {
                "freq": "monthly",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_ct_no": {
                "freq": "monthly",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_ct_tra": {
                "freq": "monthly",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_cv207": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_cv207_cumulative": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_nops": {
                "freq": "monthly",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_spi_3g_subs": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_thdd": {
                "freq": "monthly",
                "threshold": "0.05"
            },
            "blueinfo_dbvnp_prepaid_subscribers_history": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ggsn": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_air": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_crs_disconnected_subscriber": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_crs_usage": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_crs_usageda": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_sdp_dedicatedaccount": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ocs_sdp_subscriber": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_rims_celllac": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_smrs_dwd_geo_rvn_mtd": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_tac_gsma": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_vascdr_2friend_log": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_vascdr_brandname_meta": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_vascdr_udv_credit_log": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_vascdr_utn_credit_log": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_vascloud_da": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_voice_msc": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_voice_volte": {
                "freq": "daily",
                "threshold": "0.05"
            },
            "blueinfo_ccbs_ccs_xxx_danhba_dds_pttb": {
                "freq": "daily",
                "threshold": "0.05"
            }
        }
    }
}

