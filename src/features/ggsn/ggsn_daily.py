from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
import pandas as pd
import numpy as np
import time
import sys
from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql import SparkSession
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

import os
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

### 
time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""

@status_check
def gen_ggsn_daily_fts(spark, table_name, bt_table_name, bt_msisdn_column, run_date, out_dir, run_mode, common_config): 


    query = f"""select msisdn, DURATION, DATA_VOLUME_DOWNLINK, DATA_VOLUME_UPLINK, s3_file_date, RECORD_OPENING_TIME 
                from {table_name}
                where s3_file_date = {run_date} 
            """
    
    df = utils.spark_read_data_from_singlestore(spark, query).persist()
    df.count() 

    if run_mode == 'prod': 
        df = df.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    ## transform
    df = (
        df
        .withColumn("DURATION_MINUTES", F.col("DURATION") / 60)
        .withColumn("DATA_VOLUME_DOWNLINK_MB", F.col("DATA_VOLUME_DOWNLINK") / 1000000)
        .withColumn("DATA_VOLUME_UPLINK_MB", F.col("DATA_VOLUME_UPLINK") / 1000000)
        .withColumn("hour", F.date_format("RECORD_OPENING_TIME", "hh"))
        .withColumn("is_wk", F.expr("case when dayofweek(s3_file_date) in (1, 7) then 1 else 0 end"))
        .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
    )

    ## general feature
    aggregations = [] 
    for feature in ["DURATION", "DATA_VOLUME_DOWNLINK" , "DATA_VOLUME_UPLINK"]:
        feature = feature.lower()
        aggregations.extend([
            F.sum(feature).cast(T.DoubleType()).alias(f"ggsn_{feature}_sum_daily"),
            F.count(feature).cast(T.DoubleType()).alias(f"ggsn_{feature}_count_daily"),
            F.min(feature).cast(T.DoubleType()).alias(f"ggsn_{feature}_min_daily"),
            F.max(feature).cast(T.DoubleType()).alias(f"ggsn_{feature}_max_daily"),
            F.sum(F.expr(f"case when is_wk = True then {feature} end")).alias(f"ggsn_{feature}_wk_sum_daily"),
            F.sum(F.expr(f"case when is_wk = False then {feature} end")).alias(f"ggsn_{feature}_wd_sum_daily"),
            F.sum(F.expr(f"case when time_slot_grp = 'dt' then {feature} end")).alias(f"ggsn_{feature}_dt_sum_daily"),
            F.sum(F.expr(f"case when time_slot_grp = 'nt' then {feature} end")).alias(f"ggsn_{feature}_nt_sum_daily"),
        ])

    ## add features for duration field
    aggregations.extend([
            F.sum(F.expr("case when DURATION_MINUTES > 5 then 1 else 0 end")).cast(T.DoubleType()).alias(f"ggsn_duration_count_over_5mins_daily"), 
            F.sum(F.expr("case when DURATION_MINUTES < 0.5 then 1 else 0 end")).cast(T.DoubleType()).alias(f"ggsn_duration_count_under_30secs_daily"),
    ])
        
    
    ## add other features (user behavior)
    aggregations.extend([
            F.sum(F.expr("case when is_wk = True then 1 else 0 end")).alias(f"ggsn_wk_count_daily"),
            F.sum(F.expr("case when is_wk = False then 1 else 0 end")).alias(f"ggsn_wd_count_daily"),
            F.sum(F.expr("case when DATA_VOLUME_DOWNLINK_MB = 0 then 0 else 1 end")).alias(f"ggsn_downlink_count_daily"),
            F.sum(F.expr("case when DATA_VOLUME_UPLINK_MB = 0 then 0 else 1 end")).alias(f"ggsn_uplink_count_daily")
    ])

    df_fts = df.groupBy("msisdn").agg(*aggregations)
    
    utils.save_to_s3(df_fts, out_dir + f'daily/date={run_date}')
    df.unpersist()

if __name__ == "__main__":

    table_name = 'blueinfo_ggsn'

    run_create_feature(
        func_and_kwargs=(
            (gen_ggsn_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'ggsn',

        }
    )