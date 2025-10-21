from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DoubleType, IntegerType
import os
import sys
from config_edit import config
import utils

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_dates = config.fix_date
fix_date = fix_dates[0] 
run_mode = config.run_mode

spark = utils.create_spark_instance(run_mode)

# define cac feature su dung trong model 
lst_features = {'blueinfo_ccbs_spi_3g_subs': ['f_blueinfo_ccbs_spi_3g_subs_most-frequent-percent_service_code**Filter=full_T-3-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-percent_last_action**Filter=full_T-3-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-2-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-4-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-4-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-3-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-4-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-5-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-4-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-6-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-3-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-3-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-2-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_last_action**Filter=full_T-5-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-1-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-5-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-6-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-6-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-2-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-6-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-5-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_service_code**Filter=full_T-1-acc',
  'f_blueinfo_ccbs_spi_3g_subs_least-frequent-item_last_action**Filter=full_T-1-acc',
  'f_blueinfo_ccbs_spi_3g_subs_most-frequent-item_service_code**Filter=full_T-3-acc'],
 'blueinfo_ocs_air': ['f165_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-2',
  'f191_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-3',
  'f135_blueinfo_ocs_air_std_reillamount**Filter=full_T-1',
  'f233_blueinfo_ocs_air_std_reillamount**Filter=module==AdjustmentRecord_T-5',
  'f172_blueinfo_ocs_air_sum_reillamount**Filter=full_T-3',
  'f133_blueinfo_ocs_air_min_reillamount**Filter=full_T-1',
  'f131_blueinfo_ocs_air_avg_reillamount**Filter=full_T-1',
  'f228_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-5',
  'f245_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-6',
  'f174_blueinfo_ocs_air_count_reillamount**Filter=full_T-3',
  'f175_blueinfo_ocs_air_min_reillamount**Filter=full_T-3',
  'f221_blueinfo_ocs_air_sum_reillamount**Filter=module==RefillRecord_T-5',
  'f257_blueinfo_ocs_air_ratio_reillamount**Filter=module==RefillRecord_T-2',
  'f256_blueinfo_ocs_air_ratio_reillamount**Filter=full_T-2',
  'f186_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-3',
  'f154_blueinfo_ocs_air_min_reillamount**Filter=full_T-2',
  'f132_blueinfo_ocs_air_count_reillamount**Filter=full_T-1',
  'f130_blueinfo_ocs_air_sum_reillamount**Filter=full_T-1',
  'f182_blueinfo_ocs_air_min_reillamount**Filter=module==RefillRecord_T-3',
  'f249_blueinfo_ocs_air_sum_reillamount**Filter=module==AdjustmentRecord_T-6',
  'f258_blueinfo_ocs_air_ratio_reillamount**Filter=module==AdjustmentRecord_T-2',
  'f166_blueinfo_ocs_air_avg_reillamount**Filter=module==AdjustmentRecord_T-2',
  'f169_blueinfo_ocs_air_max_reillamount**Filter=module==AdjustmentRecord_T-2',
  'f142_blueinfo_ocs_air_std_reillamount**Filter=module==RefillRecord_T-1'],
 'blueinfo_ccbs_cv207': ['f4204_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-3',
  'f4195_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-2',
  'f4187_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-1',
  'f4194_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-2',
  'f4205_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-3',
  'f4212_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-4',
  'f4202_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-3',
  'f4216_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-5',
  'f4211_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-4',
  'f4201_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-3',
  'f4203_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-3',
  'f4218_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-5',
  'f4209_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-4',
  'f4188_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-1',
  'f4222_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-6',
  'f4223_blueinfo_ccbs_cv207_most-frequent-item_ma_tinh**Filter=full_T-6',
  'f4189_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-1',
  'f4208_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-4',
  'f4190_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-1',
  'f4197_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-2',
  'f4210_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-4',
  'f4196_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-2',
  'f4226_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-6',
  'f4215_blueinfo_ccbs_cv207_least-frequent-item_ma_tinh**Filter=full_T-5',
  'f4224_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-6',
  'f4225_blueinfo_ccbs_cv207_most-frequent-value_ma_tinh**Filter=full_T-6',
  'f4219_blueinfo_ccbs_cv207_least-frequent-value_ma_tinh**Filter=full_T-5',
  'f4217_blueinfo_ccbs_cv207_unique_ma_tinh**Filter=full_T-5'],
 'device': ['device_ram_category',
  'device_internal_storage',
  'device_chipset_family',
  'device_lpwan',
  'device_gpu_family',
  'device_32_bit'],
 'blueinfo_ggsn': ['f35_blueinfo_ggsn_max_DATA_VOLUME_UPLINK_MB**Filter=full_T-2',
  'f5_blueinfo_ggsn_max_DURATION_MINUTES**Filter=full_T-1',
  'f6_blueinfo_ggsn_std_DURATION_MINUTES**Filter=full_T-1',
  'f1_blueinfo_ggsn_sum_DURATION_MINUTES**Filter=full_T-1',
  'f12_blueinfo_ggsn_std_DATA_VOLUME_DOWNLINK_MB**Filter=full_T-1',
  'f15_blueinfo_ggsn_count_DATA_VOLUME_UPLINK_MB**Filter=full_T-1',
  'f3_blueinfo_ggsn_count_DURATION_MINUTES**Filter=full_T-1',
  'f40_blueinfo_ggsn_min_DURATION_MINUTES**Filter=full_T-3',
  'f32_blueinfo_ggsn_avg_DATA_VOLUME_UPLINK_MB**Filter=full_T-2',
  'f113_blueinfo_ggsn_ratio_DATA_VOLUME_DOWNLINK_MB**Filter=full_T-3',
  'f22_blueinfo_ggsn_min_DURATION_MINUTES**Filter=full_T-2',
  'f55_blueinfo_ggsn_sum_DURATION_MINUTES**Filter=full_T-4'],
 'blueinfo_ccbs_ct_no': ['f3819_blueinfo_ccbs_ct_no_count_THUE**Filter=full_T-3',
  'f3813_blueinfo_ccbs_ct_no_count_NOGOC**Filter=full_T-3'],
 'prepaid_and_danhba': ['Sex', 'Age_group'],
 'blueinfo_ocs_sdp_subscriber': ['sub_balance_pct_day_50k_to_100k_l4w',
  'sub_balance_pct_day_lt_10k_l1w_vs_l4w',
  'sub_balance_pct_day_lt_5k_l1w_vs_l4w',
  'sub_balance_pct_day_ge_100k_l4w_vs_l12w',
  'sub_balance_pct_day_5k_to_10k_l4w',
  'sub_balance_pct_day_20k_to_50k_l4w_vs_l12w',
  'sub_activated_flag', # huy them de filter leadgen
  'sub_balance_pct_day_lt_5k_l4w_vs_l12w'],
 'blueinfo_ocs_crs_usage': ['f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-3-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-1-acc',
  'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-1-acc',
  'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==IMS_T-1-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==_T-3-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-2-acc',
  'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-3-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-2-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-1-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
  'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
  'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-3-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==VOICE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_ratio_totalcost**Filter=CHARGINGCATEGORYNAME==VoLTE_T-2-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-2-acc',
  'f_blueinfo_ocs_crs_usage_avg_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-3-acc',
  'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SMS_T-1-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-4-acc',
  'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==SERVICE_CHARGING (SCAP)_T-4-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-4-acc',
  'f_blueinfo_ocs_crs_usage_sum_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
  'f_blueinfo_ocs_crs_usage_count_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-4-acc',
  'f_blueinfo_ocs_crs_usage_std_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-3-acc',
  'f_blueinfo_ocs_crs_usage_max_totalcost**Filter=CHARGINGCATEGORYNAME==DATA_T-5-acc'],
 'blueinfo_vascdr_2friend_log': [],
 'blueinfo_vascdr_brandname_meta': ['f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=finance_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=full_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-value_SENDER**Filter=full_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=full_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-value_SENDER**Filter=full_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=finance_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=full_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-percent_SENDER**Filter=full_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-percent_SENDER**Filter=finance_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=full_T-6-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-5-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-4-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-4-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-6-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-6-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-4-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-4-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-5-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-6-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=full_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-3-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=full_T-5-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-2-acc',
  'f_blueinfo_vascdr_brandname_meta_most-frequent-item_SENDER**Filter=finance_T-5-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-1-acc',
  'f_blueinfo_vascdr_brandname_meta_least-frequent-item_SENDER**Filter=finance_T-6-acc',
  'f_blueinfo_vascdr_brandname_meta_unique_SENDER**Filter=finance_T-6-acc'],
 'blueinfo_vascdr_udv_credit_log': [],
 'blueinfo_vascdr_utn_credit_log': ['f4404_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-2',
  'f4430_blueinfo_vascdr_utn_credit_log_ratio_CREDIT_AMOUNT**Filter=full_T-2',
  'f4401_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-2',
  'f4431_blueinfo_vascdr_utn_credit_log_ratio_CREDIT_AMOUNT**Filter=full_T-3',
  'f4419_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-5',
  'f4428_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-6',
  'f4422_blueinfo_vascdr_utn_credit_log_max_CREDIT_AMOUNT**Filter=full_T-5',
  'f4425_blueinfo_vascdr_utn_credit_log_avg_CREDIT_AMOUNT**Filter=full_T-6'],
 'blueinfo_vascloud_da': ['f4446_blueinfo_vascloud_da_std_value**Filter=full_T-2',
  'f4447_blueinfo_vascloud_da_sum_value**Filter=full_T-3',
  'f4451_blueinfo_vascloud_da_max_value**Filter=full_T-3',
  'f4465_blueinfo_vascloud_da_sum_value**Filter=full_T-6',
  'f4445_blueinfo_vascloud_da_max_value**Filter=full_T-2',
  'f4459_blueinfo_vascloud_da_sum_value**Filter=full_T-5'],
 'blueinfo_voice_msc': ['f4730_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=ROA_T-2',
  'f4528_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-2',
  'f4735_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
  'f4571_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
  'f4487_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
  'f4563_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-3',
  'f4480_blueinfo_voice_msc_max_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
  'f4728_blueinfo_voice_msc_ratio_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
  'f4485_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
  'f4477_blueinfo_voice_msc_avg_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
  'f4688_blueinfo_voice_msc_count_CALL_DURATION*s3_file_date*Filter=MOC_T-6',
  'f4483_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MTC_T-1',
  'f4570_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-3',
  'f4476_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
  'f4495_blueinfo_voice_msc_std_CALL_DURATION*s3_file_date*Filter=ROA_T-1',
  'f4479_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-1',
  'f4521_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MOC_T-2',
  'f4560_blueinfo_voice_msc_sum_CALL_DURATION*s3_file_date*Filter=MOC_T-3',
  'f4486_blueinfo_voice_msc_min_CALL_DURATION*s3_file_date*Filter=MTC_T-1'],
 'blueinfo_voice_volte': ['f4938_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MOC_T-2',
  'f4798_blueinfo_voice_volte_max_call_duration*s3_file_date*Filter=MOC_T-3',
  'f4766_blueinfo_voice_volte_count_call_duration*s3_file_date*Filter=MTC_T-2',
  'f4944_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MOC_T-3',
  'f4767_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MTC_T-2',
  'f4764_blueinfo_voice_volte_sum_call_duration*s3_file_date*Filter=MTC_T-2',
  'f4797_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MOC_T-3',
  'f4939_blueinfo_voice_volte_ratio_call_duration*s3_file_date*Filter=MTC_T-2',
  'f4731_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MTC_T-1',
  'f4732_blueinfo_voice_volte_max_call_duration*s3_file_date*Filter=MTC_T-1',
  'f4761_blueinfo_voice_volte_min_call_duration*s3_file_date*Filter=MOC_T-2',
  'f4795_blueinfo_voice_volte_avg_call_duration*s3_file_date*Filter=MOC_T-3',
  'f4794_blueinfo_voice_volte_sum_call_duration*s3_file_date*Filter=MOC_T-3'],
 'blueinfo_smrs_dwd_geo_rvn_mtd_part1': ['f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_ELOAD_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_FREE_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_ELOAD_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_FREE_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_RCV_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_RCV_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_BLLD_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_BLLD_USG*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_FREE_USG*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DISC_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_ELOAD_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_RCV_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DISC_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_RCHRG_PRMTN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_OFFNET_IC_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_RCV_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DISC_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DISC_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_BLLD_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_BLLD_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_USG*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_ELOAD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_BLLD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_RVN_AMT*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_ELOAD_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_BLLD_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_ELOAD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_RC_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_VOICE_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_IC_USG*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_BLLD_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_ONNET_OG_USG*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_RVN_AMT*s3_file_date*Filter=full_T-6-acc'],
 'blueinfo_smrs_dwd_geo_rvn_mtd_part2': ['f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_USG*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_SMS_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VOICE_ONNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_VOICE_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_VOICE_OFFNET_OG_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_USG*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_DATA_USG*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_DATA_USG*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_DATA_RVN_AMT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_DATA_CHRGD_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_USG*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_TOT_VOICE_RVN_AMT*s3_file_date*Filter=full_T-2-acc'],
 'blueinfo_smrs_dwd_geo_rvn_mtd_part3': ['f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_TOT_SMS_OFFNET_OG_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_TOT_VAS_RVN_AMT*s3_file_date*Filter=full_T-6-acc'],
 'blueinfo_smrs_dwd_geo_rvn_mtd_part4': ['f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_SMS_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_DATA_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_DATA_EVT_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_BLLD_USG_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_min_SMS_CNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_CNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_ONNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_SNT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_avg_SMS_CNT*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_CNT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_EVT_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_max_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_std_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_CNT*s3_file_date*Filter=full_T-3-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_SMS_ONNET_IC_CNT*s3_file_date*Filter=full_T-2-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-4-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_count_DATA_BYTES_SNT*s3_file_date*Filter=full_T-1-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_CNT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_SMS_OFFNET_OG_CNT*s3_file_date*Filter=full_T-6-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_sum_DATA_BYTES_RCVD*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_CNT*s3_file_date*Filter=full_T-5-acc',
  'f_blueinfo_smrs_dwd_geo_rvn_mtd_ratio_SMS_OFFNET_IC_CNT*s3_file_date*Filter=full_T-6-acc'],
 'blueinfo_tac_gsma': ['Year_Released',
  'Standardised_Device_Vendor',
  'Standardised_Full_Name',
  'Standardised_Marketing_Name',
  'Total_Ram',
  'GSMA_Operating_System'],
 'blueinfo_ccbs_ct_tra': ['f3929_blueinfo_ccbs_ct_tra_sum_TRAGOC**Filter=full_T-6',
  'f3939_blueinfo_ccbs_ct_tra_max_TRATHUE**Filter=full_T-6',
  'f3940_blueinfo_ccbs_ct_tra_std_TRATHUE**Filter=full_T-6']}

key_columns = ['msisdn']
if run_mode == 'backtest': 
    key_columns.append('date')
print(f"key_columns: {key_columns}")

# Full table list
full_table_list = [
    'blueinfo_ccbs_ct_no',
    'blueinfo_ccbs_ct_tra',
    'blueinfo_ccbs_cv207',
    'blueinfo_ccbs_spi_3g_subs',
    'blueinfo_ggsn',
    'blueinfo_ocs_air',
    'blueinfo_ocs_crs_usage',
    'blueinfo_vascdr_2friend_log',
    'blueinfo_vascdr_brandname_meta',
    'blueinfo_vascdr_udv_credit_log',
    'blueinfo_vascdr_utn_credit_log',
    'blueinfo_vascloud_da',
    'blueinfo_voice_msc',
    'blueinfo_voice_volte',
    # 'blueinfo_tac_gsma',
#     'prepaid_and_danhba',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part1',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part2',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part3',
    'blueinfo_smrs_dwd_geo_rvn_mtd_part4',
#     'device',
#     'sub'
]
full_table_list = list(map(lambda x: x , full_table_list))

print("====== Looking for merge version ======")
# Find merged version
full_tables = {}
for table_name_with_version in full_table_list:
    if run_mode == 'prod': 
        merged_file = f"merged/date={ALL_MONTHS[0] + fix_date}"
    else: 
        merged_file = f"merged/"
    #### check merged_file exist
    try:
        utils.load_from_s3(spark, table_name_with_version, merged_file, run_mode)
    except:
        print(f'{table_name_with_version}/{merged_file} not exits')
        raise Exception(f'{table_name_with_version}/{merged_file} not exist')
        break
    full_tables[table_name_with_version] = merged_file
    print(f"{table_name_with_version} : {merged_file}")


# load and fill na
def fill_na_numeric(df, columns):
    df = df.fillna({col : 0.0 for col in columns})
    return df
  
def fill_na_category(df, columns):
    df = df.fillna({col: 'null' if '-item' in col else 0.0 for col in columns})
    return df

def fill_na_tac_gsma(df, columns):
    fill_na_stratery = {
        'GSMA_Operating_System': 'null',
        'Standardised_Full_Name': 'null',
        'Standardised_Device_Vendor': 'null',
        'Standardised_Marketing_Name': 'null',
        'Year_Released': '0',
        'Internal_Storage_Capacity': 0,
        'Expandable_Storage': '0',
        'Total_Ram': '0',
    }
    fill_values = {col: fill_na_stratery[col] for col in columns}
    df = df.fillna(fill_values)
    return df

def fill_na_prepaid_and_danhba(df, columns):
    fill_na_stratery = {'Sex': 2 ,'Age_group': 'ukn'}
    fill_values = {col: fill_na_stratery[col] for col in columns}
    df = df.fillna(fill_values)
    return df


# Dictionary of table prefixes and corresponding (features_key, fill_function)
table_mapping = {
    "fill_na_category" : ['blueinfo_ccbs_cv207', 
                          'blueinfo_ccbs_spi_3g_subs',
                          'blueinfo_vascdr_brandname_meta'],
    "fill_na_prepaid_and_danhba" : ['prepaid_and_danhba'],
    "fill_na_tac_gsma" : ['blueinfo_tac_gsma'],
    "fill_na_numeric" : ['blueinfo_ccbs_ct_no',
                         'blueinfo_ccbs_ct_tra',
                         'blueinfo_ggsn',
                         'blueinfo_ocs_air',
                         'blueinfo_ocs_crs_usage',
                         'blueinfo_vascdr_2friend_log',
                         'blueinfo_vascdr_udv_credit_log',
                         'blueinfo_vascdr_utn_credit_log',
                         'blueinfo_vascloud_da',
                         'blueinfo_voice_msc',
                         'blueinfo_voice_volte',
                         'blueinfo_smrs_dwd_geo_rvn_mtd_part1',
                         'blueinfo_smrs_dwd_geo_rvn_mtd_part2',
                         'blueinfo_smrs_dwd_geo_rvn_mtd_part3',
                         'blueinfo_smrs_dwd_geo_rvn_mtd_part4'],
}

print("====== Merge tables process ======")
print(f"Merge {list(full_tables.keys())[0]} table")
table_name = list(full_tables.keys())[0]
col_selected = key_columns + lst_features[table_name]
if table_name == 'blueinfo_tac_gsma': 
    col_selected += ['TAC']
df_1 = utils.load_from_s3(spark, table_name, full_tables[table_name], run_mode).select(*col_selected).filter('msisdn is not null')
df_1 = df_1.dropDuplicates(key_columns)

for i, table_name in enumerate(list(full_tables.keys())[1:]):
    print(f"Merge {table_name} table")
    col_selected = key_columns + lst_features[table_name]
        
    if table_name == 'blueinfo_tac_gsma': 
        col_selected += ['TAC']

    try: 
        df_2 = utils.load_from_s3(spark, table_name, full_tables[table_name], run_mode).select(*col_selected).filter('msisdn is not null')
    except: 
        df_2 = utils.load_from_s3(spark, table_name, full_tables[table_name], run_mode).filter('msisdn is not null')
        df_2_columns = df_2.columns
        
        col_mappings = {} 
        for col in col_selected:
            if col not in df_2_columns: 
                if col == 'date':
                    continue
                old_col = col.split("blueinfo")[1:]
                for col2 in df_2_columns: 
                    new_col = col2.split("blueinfo")[1:]
                    if old_col == new_col: 
                        col_mappings[col2] = col

        for k, v in col_mappings.items(): 
            df_2 = df_2.withColumnRenamed(k, v)    
            
        df_2 = df_2.select(*col_selected)  
    if table_name == 'blueinfo_tac_gsma': 
        df_2 = df_2.dropDuplicates("msisdn")
    else: 
        df_2 = df_2.dropDuplicates(key_columns)         

    # join 
    df_1 = df_1.join(df_2, key_columns, "fullouter")
    

    print("Save checkpoint!!!!")
    df_1 = df_1.checkpoint()
    ### check distinct
    _c = df_1.agg(F.count("*").alias("_c"),
                    F.countDistinct(*key_columns).alias("_c_distinct")).toPandas()
    if _c['_c'].iloc[0] != _c['_c_distinct'].iloc[0]:
        raise ValueError(f"Data joined has been duplicated!!! -- index: {i}")

## fill na
print("====== Fill na process ======")
for _type, tables in table_mapping.items():
    columns = [] 
    for table in tables: 
        columns.extend(lst_features[table])
    if _type == 'fill_na_category': 
        df_1 = fill_na_category(df_1, columns)
#     elif _type == 'fill_na_prepaid_and_danhba': 
#         df_1 = fill_na_prepaid_and_danhba(df_1, columns)
    # elif _type == 'fill_na_tac_gsma': 
    #     df_1 = fill_na_tac_gsma(df_1, columns)
    elif _type == 'fill_na_numeric': 
        df_1 = fill_na_numeric(df_1, columns)

# join with tac gsma
if run_mode == 'prod': 
    tac_gsma = utils.load_from_s3(spark, 'blueinfo_tac_gsma' , f"merged/date={ALL_MONTHS[0] + fix_date}", run_mode)
    tac_gsma = fill_na_tac_gsma(tac_gsma, lst_features['blueinfo_tac_gsma'])
    col_selected = key_columns + lst_features['blueinfo_tac_gsma'] + ['TAC']
    tac_gsma = tac_gsma.select(*col_selected)
else: 
    tac_gsma = utils.load_from_s3(spark, 'blueinfo_tac_gsma' , f"merged", run_mode)
    tac_gsma = tac_gsma.select("msisdn", "TAC")

tac_gsma = tac_gsma.dropDuplicates(["msisdn"])
df_1 = df_1.join(tac_gsma, "msisdn", "left")

## merge special cases (sub, device, prepaid danh ba) 
if run_mode == 'prod': 
    prepaid_and_danhba = utils.load_from_s3(spark, 'prepaid_and_danhba' , f"merged/date={ALL_MONTHS[0] + fix_date}", run_mode)
else: 
    prepaid_and_danhba = utils.load_from_s3(spark, 'prepaid_and_danhba' , f"merged", run_mode)

pp_db_cols = lst_features['prepaid_and_danhba']
prepaid_and_danhba = fill_na_prepaid_and_danhba(prepaid_and_danhba, pp_db_cols)

## merge sub and device 
sub = utils.load_from_s3(spark, 'sub' , f"final_fts/date={ALL_MONTHS[0] + fix_date}", run_mode)
device = utils.load_from_s3(spark, 'device' , f"date={ALL_MONTHS[0] + fix_date}", run_mode)
device = device.where("TAC is not null")

df_1 = df_1.join(prepaid_and_danhba, "msisdn", how = "left").join(sub, "msisdn", how='left')
df_1 = df_1.join(device, ["TAC"], how='left')

# ## save data
target_file_name = config.cs_data_dir
df_1.write.parquet(target_file_name, mode='overwrite')