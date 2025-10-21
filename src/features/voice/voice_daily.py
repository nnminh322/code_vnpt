from pyspark.sql import SparkSession, functions as F, types as T
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


is_wk_str = "dayofweek(timestamps) in (1, 7)"
time_slot_str = """
    case
        when (hour < 6) or (hour >= 18) then 'nt'
        when (hour >= 6) and (hour < 12) then 'mo'
        when (hour >= 12) and (hour < 18) then 'af_ev'
    end
"""

time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""

call_not_answered_str = """
    case
        when (CALL_DURATION = 0) then 1
        else 0
    end
"""

@status_check
def gen_voice_daily_fts(spark, out_dir, run_date, run_mode, table_name, bt_table_name, bt_msisdn_column, common_config):
    freq_str = "daily"
    
    print("generating daily fts for:", run_date)
    
    call_type = {
        "call_in": "MTC",
        "call_out": "MOC",
        "roam": "ROA",
        "cf": "CF"
    }
    
    sms_type = {
        "sms_in": "SMT",
        "sms_out": "SMO"
    }
    
    super_dict = {
        "call": call_type,
        "sms": sms_type
    }
    
    for sub_group, _dict in super_dict.items():
        for _type, _filter in _dict.items():
            
            # load data
            query = f"""
                SELECT MSISDN, CALL_DURATION, CALL_TYPE, 
                    LOCATION_AREA as lac, CELL_ID as cell_id,
                    s3_file_date
                FROM {table_name} 
                WHERE 1=1
                    AND CALL_TYPE = '{_filter}'
                    AND s3_file_date = '{run_date}'
            """

            df_voice_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
            df_voice_raw.count()

            if run_mode == 'prod': 
                df_voice_raw = df_voice_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")

            df_voice = (
                df_voice_raw
                    .withColumnRenamed("MSISDN", "msisdn")
                    .withColumn("timestamps", F.to_date("s3_file_date", "yyyyMMdd"))
                    .withColumn("is_wk", F.expr(is_wk_str))
                    .withColumn("call_not_answered", F.expr(call_not_answered_str))
            )

            # create features
            aggs = [
                F.count("*").alias(f"voice_{_type}_count_{freq_str}"),
                F.sum(F.expr("case when is_wk = True then 1 end")).alias(f"voice_{_type}_count_wk_{freq_str}"),
                F.sum(F.expr("case when is_wk = False then 1 end")).alias(f"voice_{_type}_count_wd_{freq_str}")                
            ]
            
            if sub_group == "call":
                aggs.extend(
                    [
                        F.sum("CALL_DURATION").alias(f"voice_{_type}_duration_sum_{freq_str}"),
                        F.sum(F.expr("case when is_wk = True then CALL_DURATION end")).alias(f"voice_{_type}_duration_sum_wk_{freq_str}"),
                        F.sum(F.expr("case when is_wk = False then CALL_DURATION end")).alias(f"voice_{_type}_duration_sum_wd_{freq_str}"),
                        F.sum(F.expr("case when call_not_answered = 1 then 1 end")).alias(f"voice_{_type}_not_answered_count_{freq_str}")
                    ]
                )

            df_voice = df_voice.groupBy("msisdn").agg(*aggs)
            utils.save_to_s3(df_voice, out_dir + f"/{freq_str}/{sub_group}/{_type}/date={run_date}")

    # load saved features
    dfs = []
    for sub_group, _dict in super_dict.items():
        for _type, _filter in _dict.items():
            try:
                df = utils.load_from_s3(spark,out_dir + f"/{freq_str}/{sub_group}/{_type}/date={run_date}")
                dfs.append(df)
            except Exception as e:
                print(e)
                break
                print("skip")
    df_fts = dfs[0]

    for _df in dfs[1:]:
        df_fts = df_fts.join(_df, ["msisdn"], how="outer")
    
    # calculate more features
    total_call_str = " + ".join([f"voice_{_type}_count_{freq_str}" for _type in call_type.keys()])
    total_call_wk_str = " + ".join([f"voice_{_type}_count_wk_{freq_str}" for _type in call_type.keys()])
    total_call_wd_str = " + ".join([f"voice_{_type}_count_wd_{freq_str}" for _type in call_type.keys()])

    total_sms_str = " + ".join([f"voice_{_type}_count_{freq_str}" for _type in sms_type.keys()])
    total_sms_wk_str = " + ".join([f"voice_{_type}_count_wk_{freq_str}" for _type in sms_type.keys()])
    total_sms_wd_str = " + ".join([f"voice_{_type}_count_wd_{freq_str}" for _type in sms_type.keys()])
    
    sum_call_not_answered_str = " + ".join([f"voice_{_type}_not_answered_count_{freq_str}" for _type in call_type.keys()])

    df_fts = df_fts.withColumn(f"voice_total_call_count_{freq_str}", F.expr(total_call_str))\
                   .withColumn(f"voice_total_call_wk_count_{freq_str}", F.expr(total_call_wk_str))\
                   .withColumn(f"voice_total_call_wd_count_{freq_str}", F.expr(total_call_wd_str))\
                   .withColumn(f"voice_total_sms_count_{freq_str}", F.expr(total_sms_str))\
                   .withColumn(f"voice_total_sms_wk_count_{freq_str}", F.expr(total_sms_wk_str))\
                   .withColumn(f"voice_total_sms_wd_count_{freq_str}", F.expr(total_sms_wd_str))\
                   .withColumn(f"voice_total_count_{freq_str}", F.expr(f"voice_total_call_count_{freq_str} + voice_total_sms_count_{freq_str}"))\
                   .withColumn(f"voice_total_call_not_answered_count_{freq_str}", F.expr(sum_call_not_answered_str))
    
    # save to parquet
    utils.save_to_s3(df_fts, out_dir + f"/{freq_str}/total_features/date={run_date}")

    df_voice_raw.unpersist()


if __name__ == "__main__":

    table_name = 'blueinfo_voice_msc'

    run_create_feature(
        func_and_kwargs=(
            (gen_voice_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'voice',

        }
    )
    