import re
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
import pandas as pd
import numpy as np
import time
import sys
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

import os
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

feature_groups = {
    "group_1" : {
            'TOT_RVN_AMT' : 'total_revenue',
            'TOT_BLLD_AMT' : 'total_charge',
            'TOT_DISC_AMT': 'total_discount_usage', 
            'TOT_RC_RVN_AMT': 'total_auto_renewal_revenue',
            'RCHRG_PRMTN_AMT': 'recharge_amount',
            'ELOAD_AMT': 'eload_amount',
            'RCV_AMT': 'paper_amount',
            'ELOAD_EVT_CNT': 'eload_attempt',
            'VOICE_USG': 'call_usage',
            'VOICE_FREE_USG': 'free_call',
            'VOICE_BLLD_USG': 'paid_call',
            'VOICE_ONNET_IC_USG': 'call_in_onnet',
            'VOICE_ONNET_OG_USG': 'call_out_onnet',
            'VOICE_OFFNET_IC_USG' : 'call_in_offnet',
            'VOICE_OFFNET_OG_USG' : 'call_out_offnet',
            'VOICE_ISD_OG_USG' : 'call_international',
        }, 
    "group_2" : {
            'DATA_USG' : 'data_usage',
            'TOT_VOICE_RVN_AMT' : 'voice_revenue',
            'TOT_VOICE_REFUND_AMT' : 'voice_refund_revenue',
            'TOT_VOICE_DISC_AMT' : 'voice_discount_revenue',
            'TOT_VOICE_ONNET_IC_RVN_AMT' : 'call_in_onnet_revenue',
            'TOT_VOICE_ONNET_OG_RVN_AMT' : 'call_out_onnet_revenue',
            'TOT_VOICE_OFFNET_IC_RVN_AMT' : 'call_in_offnet_revenue',
            'TOT_VOICE_OFFNET_OG_RVN_AMT' : 'call_out_offnet_revenue',
            'TOT_VOICE_ISD_OG_RVN_AMT' : 'call_international_out_revenue',
            'TOT_DATA_RVN_AMT' : 'data_revenue',
            'TOT_DATA_CHRGD_AMT' : 'data_recharge_revenue',
            'TOT_SMS_RVN_AMT' : 'sms_revenue',
            'TOT_SMS_REFUND_AMT' : 'sms_refund_revenue',
            'TOT_SMS_ONNET_OG_RVN_AMT' : 'sms_out_onnet_revenue',
            'TOT_SMS_OFFNET_OG_RVN_AMT' : 'sms_out_offnet_revenue',
            'TOT_SMS_ISD_OG_RVN_AMT' : 'sms_international_out_revenue',
        },
    "group_3" : {
            'TOT_MMS_RVN_AMT' : 'mms_revenue',
            'TOT_MMS_NRC_RVN_AMT' : 'mms_non_auto_renewal_revenue',
            'TOT_VAS_DISC_AMT' : 'vas_discount_revenue',
            'TOT_OTHRS_RVN_AMT' : 'others_revenue',
            'TOT_OTHERS_RC_RVN_AMT' : 'others_auto_renewal_revenue',
            'SMS_FREE_USG_CNT' : 'sms_free_attempt',
            'SMS_BLLD_USG_CNT' : 'sms_paid_attemtp',
            'SMS_ONNET_IC_CNT' : 'sms_in_onnet_attempt',
            'SMS_ONNET_OG_CNT' : 'sms_out_onnet_attempt',
            'SMS_OFFNET_IC_CNT' : 'sms_in_offnet_attempt',
            'SMS_OFFNET_OG_CNT' : 'sms_out_offnet_attempt',
            'SMS_ISD_OG_CNT' : 'sms_international_out_attempt',
            'SMS_ISD_IC_CNT' : 'sms_international_in_attempt',
            'DATA_EVT_CNT' : 'data_attempt',
            'DATA_BYTES_RCVD' : 'data_upload_amount',
            'DATA_BYTES_SNT' : 'data_download_amount',
            'SMS_CNT' : 'sms_attempt'
            }
}

def get_n_previous_month(date_str, n):
    date = datetime.strptime(date_str, "%Y%m%d")
    return (date - relativedelta(months=n)).strftime("%Y%m")

@status_check
def gen_smrs_lxm_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config, level):
    print("generating lxm fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")

    # if level = weekly then generate l1m features
    # else generate lxm features --> difference directory
    for i in [1, 2, 3, 6]:
        # get date from / date to / freq_str
        freq_str = f"l{i}m"

        month_prev = get_n_previous_month(run_date, i)
        
        max_date = utils.spark_read_data_from_singlestore(spark, f"select max(s3_file_date) max_date from {table_name} where MO_KEY = {month_prev}").toPandas()['max_date'].values[0]

        for group_name, features_dct in feature_groups.items(): 
            features = list(features_dct.keys())
            print(f"--fts {freq_str} with level = {level} | group = {group_name}")
            
            if max_date is None: 
                query = f"select ACCS_MTHD_KEY as msisdn, {', '.join(features)} from {table_name} where 1 = 0"
            else: 
                query = f"select ACCS_MTHD_KEY as msisdn, {', '.join(features)} from {table_name} where s3_file_date = {max_date}"

            if i == 1: 
                if level == 'monthly':
                    df_raw = utils.spark_read_data_from_singlestore(spark, query).persist() 
                    df_raw.count() 

                    if run_mode == 'prod': 
                        df_raw = df_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
                    df_fts = df_raw.groupBy("msisdn").agg(*[F.sum(F.col(feature)).alias(f"smrs_{features_dct[feature]}_sum_{freq_str}")
                                                            for feature in features])
                else: continue
            else: 
                if level != 'monthly':
                    date_list = [run_date]
                    for step in range(1, i):
                        date_from = date_to - relativedelta(weeks=step*4)  # fixed: monday
                        date_from_str = date_from.strftime("%Y%m%d")
                        date_list.append(date_from_str)

                    lower_fts = utils.load_from_s3(spark, out_dir + f"/l1m/group={group_name}", "true").filter(F.col("date").isin(date_list)).persist()
                    lower_fts.count()
                    lower_fts = lower_fts.drop("group")

                    old_suffix = "l1m"
                    new_suffix = freq_str

                    # create sum count features
                    exclude_list = ["msisdn", "date"]
                    fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
                    
                    # break down due to huge aggregations 
                    aggs = [
                        [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_sum_"])] + 
                        [F.min(F.col(x)).alias(x.replace('_sum_', '_min_').replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_sum_"])] + 
                        [F.max(F.col(x)).alias(x.replace('_sum_', '_max_').replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_sum_"])] + 
                        [F.avg(F.col(x)).alias(x.replace('_sum_', '_avg_').replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_sum_"])] 
                    ]

                    df_fts = None
                    for agg in aggs: 
                        df_1 = lower_fts.groupBy("msisdn").agg(*agg)
                        df_fts = df_1 if df_fts is None else df_fts.join(df_1, "msisdn", "outer")

                    lower_fts.unpersist()
                else: continue

            # write to parquet
            utils.save_to_s3(df_fts, out_dir + f"/{freq_str}/group={group_name}/date={run_date}")
            if i == 1: 
                if level == 'monthly':
                    df_raw.unpersist()

@status_check
def merge_smrs_fts(spark, out_dir, run_date,  **kwargs):
   
    print("merging lxm fts for", run_date)

    for group_name in feature_groups: 
        l1m = utils.load_from_s3(spark, out_dir + f"/l1m/group={group_name}", "true").where(f"date='{run_date}'").drop("date", "group")
        l2m = utils.load_from_s3(spark, out_dir + f"/l2m/group={group_name}", "true").where(f"date='{run_date}'").drop("date", "group")
        l3m = utils.load_from_s3(spark, out_dir + f"/l3m/group={group_name}", "true").where(f"date='{run_date}'").drop("date", "group")
        l6m = utils.load_from_s3(spark, out_dir + f"/l6m/group={group_name}", "true").where(f"date='{run_date}'").drop("date", "group")
        
        df_fts = l1m.join(l2m, on=["msisdn"], how='outer')\
                .join(l3m, on=["msisdn"], how='outer')\
                .join(l6m, on=["msisdn"], how='outer')

        # Create ratio features
        exclude_list = ["msisdn", "date"]

        fts_names = [col for col in l1m.columns if col not in exclude_list]
        fts_names = [x for x in fts_names if not any(sub in x for sub in ["_sum_"])]

        lxm_list = ['l1m', 'l2m', 'l3m', 'l6m']
        
        for ft in fts_names:
            for i in range(0, len(lxm_list)):
                for j in range(i+1, len(lxm_list)):
                    lxm = lxm_list[i]
                    lyw = lxm_list[j]
                    new_ft = ft[:ft.rfind('_')] + '_' + lxm + '_vs_' + lyw
                    lxm_ft = ft[:ft.rfind('_')] + '_' + lxm
                    lyw_ft = ft[:ft.rfind('_')] + '_' + lyw
                    df_fts = df_fts.withColumn(new_ft, F.expr(f"{lxm_ft} / {lyw_ft}"))

        # write to parquet
        utils.save_to_s3(df_fts, f"{out_dir}/final_fts/group={group_name}/date={run_date}")

if __name__ == "__main__":
    table_name = "blueinfo_smrs_dwd_geo_rvn_mtd"

    run_create_feature(
        func_and_kwargs=(
            (gen_smrs_lxm_fts, {'level':'monthly'}),
            (gen_smrs_lxm_fts, {'level':'lxm'}),
            (merge_smrs_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'smrs',
            'table_name': table_name,
        }
    )