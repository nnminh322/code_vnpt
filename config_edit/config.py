import sys
import pandas as pd
import numpy as np

from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession

import time
from datetime import datetime, timedelta

import boto3
from botocore.config import Config


#### table dev vs production
table_dict = {
    'blueinfo_ccbs_ct_no': 'blueinfo_ccbs_ct_no',
    'blueinfo_ccbs_ct_tra': 'blueinfo_ccbs_ct_tra',
    'blueinfo_ccbs_cv207' : 'blueinfo_ccbs_cv207',
    'blueinfo_ccbs_spi_3g_subs' : 'blueinfo_ccbs_spi_3g_subs',
    'blueinfo_ggsn': 'blueinfo_ggsn',
    'blueinfo_ocs_air': 'blueinfo_ocs_air',
    'blueinfo_ocs_crs_usage': 'blueinfo_ocs_crs_usage',
    'blueinfo_vascdr_2friend_log': 'blueinfo_vascdr_2friend_log',
    'blueinfo_vascdr_brandname_meta': 'blueinfo_vascdr_brandname_meta',
    'blueinfo_vascdr_udv_credit_log': 'blueinfo_vascdr_udv_credit_log',
    'blueinfo_vascdr_utn_credit_log': 'blueinfo_vascdr_utn_credit_log',
    'blueinfo_vascloud_da': 'blueinfo_vascloud_da',
    'blueinfo_voice_msc':'blueinfo_voice_msc',
    'blueinfo_voice_volte': 'blueinfo_voice_volte',
    'blueinfo_tac_gsma': 'blueinfo_tac_gsma',
    'prepaid_and_danhba': 'prepaid_and_danhba',
    'blueinfo_smrs_dwd_geo_rvn_mtd': 'blueinfo_smrs_dwd_geo_rvn_mtd',
    'blueinfo_dbvnp_prepaid_subscribers_history': 'blueinfo_dbvnp_prepaid_subscribers_history',
    'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb' : 'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb',
    'blueinfo_ocs_sdp_subscriber' : 'blueinfo_ocs_sdp_subscriber',
    'blueinfo_voice_volte_v2': 'blueinfo_voice_volte_v2',
}

backtest_table_dict = {
    'blueinfo_ccbs_ct_no': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ccbs_ct_tra': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ccbs_cv207': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ccbs_spi_3g_subs': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ggsn': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ocs_air': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ocs_crs_usage': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_vascdr_2friend_log': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_vascdr_brandname_meta': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_vascdr_udv_credit_log': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_vascdr_utn_credit_log': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_vascloud_da': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_voice_msc': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_voice_volte': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_tac_gsma': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'prepaid_and_danhba': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_smrs_dwd_geo_rvn_mtd': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_dbvnp_prepaid_subscribers_history': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_ocs_sdp_subscriber': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
    'blueinfo_voice_volte_v2': {'backtest_table_name': 'backtest_table_name', 'backtest_table_phone_name': 'backtest_table_phone_name'},
}

#### env 
proxy = "http://10.144.13.144:3129"


#### s3 storage
global s3_endpoint
s3_endpoint = 'http://access.icos.datalake.vnpt.vn:80'

global s3_access_key
s3_access_key = 'Z994bbFtOibu48HSOyyc'

global s3_secret_key
s3_secret_key =  '3sYq37oDs30OE8iv21Qh9nIFMQW5Tz80mX9cicUA'

global bucket_name
bucket_name = "media_os_blueinfo_dev_2"
bucket_name_cos = f"cos://{bucket_name}.datalake"


#### Single store
global user
user = "blueinfo_os_2"

global password
password = "UrI0avb0ds0vnJ4rz4E1"

client_name = 'SHBFC' # doi theo tung FI

## directory name
directory_name = {'prod' : 'features_v2',
                 'backtest': f'feature_bt_{client_name}'} 

# define date, month and run mode
args = sys.argv[1:]
all_months_str = next(arg for arg in args if arg.startswith('--all_months:'))
ALL_MONTHS = all_months_str.split(': ')[1][1:-1].split(', ')
fix_date_str = next(arg for arg in args if arg.startswith('--fix_date:'))
fix_date = fix_date_str.split(': ')[1][1:-1].split(', ')
run_mode = next(arg for arg in args if arg.startswith('--run_mode:')).split(': ')[1]

### config for credit score module (cs)
model_code = 'cs_lg_2025'
cs_binning_path = "models/MCmodel/opt_binning_num_4_12.pkl"
cs_model_path = "models/MCmodel/model_catboost_final.pkl"


if run_mode == 'prod':
    cs_data_dir = f"{bucket_name_cos}/scores/model_code={model_code}/merged_fts/date={ALL_MONTHS[0]}{fix_date[0]}"
    cs_output_path = f"{bucket_name_cos}/scores/model_code={model_code}/cs_predict/date={ALL_MONTHS[0]}{fix_date[0]}"
else: 
    cs_data_dir = f"{bucket_name_cos}/backtest/client={client_name}/scores/model_code={model_code}/merged_fts/date={ALL_MONTHS[0]}{fix_date[0]}"
    cs_output_path = f"{bucket_name_cos}/backtest/client={client_name}/scores/model_code={model_code}/cs_predict/"


    ## backtest
cs_data_bt_dir = f"{bucket_name_cos}/label/label_{client_name}_back_test.parquet" # doi theo tung FI


### config for leadgen score module
lg_batch_dir = f'{bucket_name_cos}/scores/model_code={model_code}/lg_batch_predict/date={ALL_MONTHS[0]}{fix_date[0]}'
lg_output_path = f'{bucket_name_cos}/scores/model_code={model_code}/lg_predict/date={ALL_MONTHS[0]}{fix_date[0]}'



## database name
db1 = 'blueinfo_dev' # database cho bang prepaid danh ba
db2 = 'blueinfo_dev' 