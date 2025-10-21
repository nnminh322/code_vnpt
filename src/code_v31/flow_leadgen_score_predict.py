from pyspark.sql import SparkSession
import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3
import joblib
from botocore.config import Config
import tempfile
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config_edit import config
import utils
import sys
import os
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
sys.path.append('/code/pythonlib')

ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
fix_date = fix_dates[0] 
run_mode = config.run_mode

utils.install_package()
import xgboost as xgb   ######### 2.1.1


m1_features = ['f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_CNT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_CNT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_CNT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-6-acc',
     'f4658_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MOC_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_CNT*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_CNT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-6-acc',
     'f4548_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MOC_T-3']

m2_features =['f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_SNT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_USG*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-5-acc',
     'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_DATA_USG*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_USG*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_DATA_USG*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_SNT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-6-acc',
     'f55_blueinfo_ggsn_sum_DURATION_MINUTES**Filter=full_T-4',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-4-acc',
     'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc']

m3_features = ['f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=finance_T-6-acc',
     'f4428_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=full_T-6-acc',
     'f4419_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-5',
     'f3940_blueinfo_ccbs_ct_tra_std_TRATHUE**Filter=full_T-6',
     'f4422_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-5',
     'f4445_blueinfo_vascloud_da_max_value**Filter=full_T-2',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
     'f4425_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-6',
     'f4451_blueinfo_vascloud_da_max_value**Filter=full_T-3',
     'f4459_blueinfo_vascloud_da_sum_value**Filter=full_T-5',
     'f4465_blueinfo_vascloud_da_sum_value**Filter=full_T-6',
     'f3929_blueinfo_ccbs_ct_tra_sum_TRAGOC**Filter=full_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
     'f3939_blueinfo_ccbs_ct_tra_max_TRATHUE**Filter=full_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
    'f4187_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-1',
     'f4188_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-1',
     'f4194_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-2',
     'f4195_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-2',
     'f4201_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-3',
     'f4202_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-3',
     'f4208_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-4',
     'f4209_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-4',
     'f4215_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-5',
     'f4216_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-5',
     'f4222_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-6',
     'f4223_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-6',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-1-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-2-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-3-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-3-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-4-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-4-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-5-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-5-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-6-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-6-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-1-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-1-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-2-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-2-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-3-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-3-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-4-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-4-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-5-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-5-acc',
     'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-6-acc',
     'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-6-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-1-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-1-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-2-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-2-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-3-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-3-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-4-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-4-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-5-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-5-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-6-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-6-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-1-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-1-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-2-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-2-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-3-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-3-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-4-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-4-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-5-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-5-acc',
     'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-6-acc',
     'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-6-acc']

m4_features = ['f214_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-5',
     'f232_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_RVN_AMT*s3_file_date*Filter=full_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_BLLD_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-4-acc',
     'f183_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-3',
     'f168_blueinfo_ocs_air_count_reillamount**Filter=full_T-3',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_BLLD_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
     'f132_blueinfo_ocs_air_count_reillamount**Filter=full_T-1',
     'f178_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-3',
     'f208_blueinfo_ocs_air_sum_reillamount**Filter=module==RefillRecord_T-5',
     'f219_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-5',
     'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-1-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_ELOAD_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
     'f229_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-6',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-6-acc',
     'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-4-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-3-acc',
     'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_ELOAD_AMT*s3_file_date*Filter=full_T-1-acc']

m5_features = [
     'f4225_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-6',
     'f4218_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-5',
     'f4204_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-3',
     'f4211_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-4',
     'f4197_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-2',
     'f4190_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-1',
     'f4189_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-1',
     'f4196_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-2',
     'f4203_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-3',
     'f4210_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-4',
     'f4217_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-5',
     'f4224_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-6',
     'f4226_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-6',
     'f4205_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-3',
     'f4219_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-5',
     'f4212_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-4',
     'GSMA_Operating_System',
     'Standardised_Full_Name',
     'Standardised_Device_Vendor',
     'Standardised_Marketing_Name',
     'Total_Ram',
     'Age_group',
     'f4187_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-1',
     'f4188_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-1',
     'f4194_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-2',
     'f4195_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-2',
     'f4201_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-3',
     'f4202_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-3',
     'f4208_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-4',
     'f4209_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-4',
     'f4215_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-5',
     'f4216_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-5',
     'f4222_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-6',
     'f4223_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-6',
    'Year_Released'
    ]


def winsorize(data, threshold):
    # handle outlier
    result = data.copy()
    threshold_dict = threshold.set_index('feature').to_dict('index')
    for feature, thresholds in threshold_dict.items():
        if feature in result.columns:
            lower_bound = thresholds['lower_threshold'] 
            upper_bound = thresholds['higher_threshold']
        
            result[feature] = result[feature].clip(lower = lower_bound, upper = upper_bound)
    return result

def scoring(df):    
    # models

    m1 = utils.load_pickle_file("models/m1/20241022_m1.parquet")
    m2 = utils.load_pickle_file("models/m2/20241022_m2.parquet")
    m3 = utils.load_pickle_file("models/m3/20241022_m3.parquet")
    m4 = utils.load_pickle_file("models/m4/20241022_m4.parquet")
    m5 = utils.load_pickle_file("models/m5/20241022_m5.parquet")
    m6 = utils.load_pickle_file("models/m6/20241022_m6.parquet")
    
    # winsorize tables
    m1_winsorize = utils.load_model_config(spark , "m1/20241022_winsorize_table.parquet").toPandas()
    m2_winsorize = utils.load_model_config(spark , "m2/20241022_winsorize_table.parquet").toPandas()
    m3_winsorize = utils.load_model_config(spark , "m3/20241022_winsorize_table.parquet").toPandas()
    m4_winsorize = utils.load_model_config(spark , "m4/20241022_winsorize_table.parquet").toPandas()
    m5_winsorize = utils.load_model_config(spark , "m5/20241022_winsorize_table.parquet").toPandas()
    
    
    component_models = [[m1_winsorize, m1_features, m1],
                       [m2_winsorize, m2_features, m2],
                       [m3_winsorize, m3_features, m3],
                       [m4_winsorize, m4_features, m4],
                       [m5_winsorize, m5_features, m5]]
    
    
    def get_score(df, component_models, m6):
                
        # Create a new column "batch_id" based on the last character of msisdn
        df = df.withColumn("batch_id", F.expr("substring(msisdn, length(msisdn)-2, 3)"))

        # Get the distinct batch_ids
        distinct_batch_ids = df.select("batch_id").distinct().collect()

        # Iterate over the distinct batch_ids and yield batches
        for batch_id_row in distinct_batch_ids:
            batch_id = batch_id_row["batch_id"]
            
            # Filter the DataFrame based on the current batch_id
            batch_df = df.where(f"batch_id = '{batch_id}'")
        
            i = 2

            # get score for m1
            winsorize_table = component_models[0][0]
            features = component_models[0][1]
            model = component_models[0][2]

            # get data
            df_fts = batch_df.select("msisdn", *features).toPandas()

            # winsorize
            m1 = winsorize(df_fts.copy(), winsorize_table)

            # get prediction
            model_id = m1[['msisdn']]
            model_score = m1.drop(columns = ['msisdn'])
            model_proba = pd.DataFrame(model.predict_proba(model_score)[:,1])
            output = pd.concat([model_id.reset_index(), model_proba], axis=1)
            output[0] = output[0].round(4)
            output[0] = output[0] * 10000
            output = output.rename(columns = {0: 'm1_score'})
            output = output.drop(columns =['index'])
            output['m1_score'] = output['m1_score'].round()


            for winsorize_table, features, model in component_models[1:]:
                # winsorize
                df_fts = batch_df.select("msisdn", *features).toPandas()
                data = winsorize(df_fts.copy(), winsorize_table)

                # get prediction
                model_id = data[['msisdn']]
                model_score = data.drop(columns = ['msisdn'])
                model_proba = pd.DataFrame(model.predict_proba(model_score)[:,1])
                output2 = pd.concat([model_id.reset_index(), model_proba], axis=1)
                output2[0] = output2[0].round(4)
                output2[0] = output2[0] * 10000
                output2 = output2.rename(columns = {0: f'm{i}_score'})
                output2 = output2.drop(columns =['index'])
                output2[f'm{i}_score'] = output2[f'm{i}_score'].round()

                # increment
                i = i + 1

                # join data
                output = output.merge(output2, on = ['msisdn'], how = 'inner')

            # get ensemble output
            model_id = output
            model_score = output.drop(columns = ['msisdn'])
            model_proba = pd.DataFrame(m6.predict_proba(model_score)[:,1])
            output = pd.concat([model_id.reset_index(), model_proba], axis=1)
            output[0] = output[0].round(4)
            output[0] = output[0] * 10000
            output = output.rename(columns = {0: 'm6_score'})
            output = output.drop(columns =['index'])
            output['m6_score'] = output['m6_score'].round()        
        
            # convert to spark dataframe
            return_spark_df = spark.createDataFrame(output)

            # save to parquet
            return_spark_df.write.mode('overwrite').parquet(f'{config.lg_batch_dir}/batch_id={batch_id}')
    get_score(df, component_models, m6)

### main 
spark = utils.create_spark_instance(run_mode)

############## load dataset
dataset = spark.read.parquet(config.cs_data_dir)

### rename column
not_exists = ['f4658_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MOC_T-6',
 'f4548_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MOC_T-3',
 'f214_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-5',
 'f232_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-6',
 'f183_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-3',
 'f168_blueinfo_ocs_air_count_reillamount**Filter=full_T-3',
 'f178_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-3',
 'f208_blueinfo_ocs_air_sum_reillamount**Filter=module==RefillRecord_T-5',
 'f219_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-5',
 'f229_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-6']

not_exists_dct = {}
for col1 in not_exists: 
    old_col = col1.split("blueinfo")[1:]
    for col2 in dataset.columns: 
        new_col = col2.split("blueinfo")[1:]
        if old_col == new_col: 
            not_exists_dct[col2] = col1
            
for k, v in not_exists_dct.items(): 
    dataset = dataset.withColumnRenamed(k, v)
############


####### Scoring
scoring(dataset)

##### load full data and calculate lg_perct_y_new
df_joined = spark.read.parquet(f"{config.lg_batch_dir}")
win = Window.orderBy(F.asc("m6_score"))
df_joined = df_joined.withColumn('lg_perct', F.ntile(100).over(win)) # lg_perct_y_new
df_joined.write.mode('overwrite').parquet(config.lg_output_path)