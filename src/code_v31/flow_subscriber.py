from pyspark.sql import functions as F, types as T
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd 
import os
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'
import sys
from pyspark.sql import SparkSession
from config_edit import config

import pandas as pd
import numpy as np
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
import time
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
import utils

def gen_sub_fts_lxw(spark, raw_table_name, snapshot_str, run_mode):
    # create lxw fts
    date_to = datetime.strptime(snapshot_str, "%Y%m%d")
    print("generating lxw fts for:", snapshot_str)
    for i in [1, 4, 12, 24]:
        
        # get date from / date to / lxw_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        lxw_str = f"l{i}w"
        print(f"--fts {lxw_str}")
        
        if run_mode == 'prod': 
            sub = utils.spark_read_data_from_singlestore(spark, f"""
                select * from {raw_table_name} where s3_file_date > '{date_from_str}' and s3_file_date <= '{date_to_str}'
            """)
        else: 
            sub = utils.spark_read_data_from_singlestore(spark, f"""
                select * from {raw_table_name} where s3_file_date > '{date_from_str}' and s3_file_date <= '{date_to_str}'
                    and account_id in (select {backtest_table_phone_name} from {backtest_table_name})
            """)
        
        sub = (
            sub.withColumn("lt_1k", F.when((F.col("account_balance") > 0) & (F.col("account_balance") < 1000), 1).otherwise(0))
               .withColumn("lt_5k", F.when((F.col("account_balance") > 0) & (F.col("account_balance") < 5000), 1).otherwise(0))
               .withColumn("lt_10k", F.when((F.col("account_balance") > 0) & (F.col("account_balance") < 10000), 1).otherwise(0))
               .withColumn("is_zero", F.when(F.col("account_balance") == 0, 1).otherwise(0))\
               .withColumn("ge_1k_lt_5k", F.when((F.col("account_balance") > 1000) & (F.col("account_balance") < 5000), 1).otherwise(0))
               .withColumn("ge_5k_lt_10k", F.when((F.col("account_balance") >= 5000) & (F.col("account_balance") < 10000), 1).otherwise(0))
               .withColumn("ge_10k_lt_20k", F.when((F.col("account_balance") >= 10000) & (F.col("account_balance") < 20000), 1).otherwise(0))
               .withColumn("ge_20k_lt_50k", F.when((F.col("account_balance") >= 20000) & (F.col("account_balance") < 50000), 1).otherwise(0))
               .withColumn("ge_50k_lt_100k", F.when((F.col("account_balance") >= 50000) & (F.col("account_balance") < 100000), 1).otherwise(0))
               .withColumn("ge_100k", F.when(F.col("account_balance") >= 100000, 1).otherwise(0))
        )
        
        # write fts
        df_fts = sub.groupby("account_id")\
                    .agg(
                         F.avg("account_balance").alias(f"sub_balance_avg_per_day_{lxw_str}"),
                         F.max("account_balance").alias(f"sub_balance_max_per_day_{lxw_str}"),
                         F.stddev("account_balance").alias(f"sub_balance_std_per_day_{lxw_str}"),
                         F.mean("lt_1k").alias(f"sub_balance_pct_day_lt_1k_{lxw_str}"),
                         F.mean("lt_5k").alias(f"sub_balance_pct_day_lt_5k_{lxw_str}"),
                         F.mean("lt_10k").alias(f"sub_balance_pct_day_lt_10k_{lxw_str}"),
                         F.mean("is_zero").alias(f"sub_balance_pct_day_zero_{lxw_str}"),
                         F.mean("ge_1k_lt_5k").alias(f"sub_balance_pct_day_1k_to_5k_{lxw_str}"),
                         F.mean("ge_5k_lt_10k").alias(f"sub_balance_pct_day_5k_to_10k_{lxw_str}"),
                         F.mean("ge_10k_lt_20k").alias(f"sub_balance_pct_day_10k_to_20k_{lxw_str}"),
                         F.mean("ge_20k_lt_50k").alias(f"sub_balance_pct_day_20k_to_50k_{lxw_str}"),
                         F.mean("ge_50k_lt_100k").alias(f"sub_balance_pct_day_50k_to_100k_{lxw_str}"),
                         F.mean("ge_100k").alias(f"sub_balance_pct_day_ge_100k_{lxw_str}")
                    )                             
    
        df_fts = df_fts.withColumnRenamed('account_id', 'msisdn')
        
        # write to parquet
#         out_path = f"{out_dir}/{lxw_str}/date={snapshot_str}"
#         df_fts.write.mode('overwrite').parquet(out_path)
        
        target_file_name = f"{lxw_str}/date={snapshot_str}"
                
        utils.save_to_s3(df_fts, 'sub' , target_file_name, run_mode)
        
    print("-"*100)


def merge_sub_fts_final(spark, raw_table_name, snapshot_str):
    
    print("merging lxw fts for", snapshot_str)

    # Get activate date in last 30d
    activate_to = datetime.strptime(snapshot_str, "%Y%m%d")
    activate_from = activate_to - relativedelta(days=30)
    activate_to_str = activate_to.strftime("%Y%m%d")
    activate_from_str = activate_from.strftime("%Y%m%d")
    if run_mode == 'backtest': 
        sub_df = utils.spark_read_data_from_singlestore(spark, f"""
            select * from {raw_table_name} where s3_file_date > '{activate_from_str}' and s3_file_date <= '{activate_to_str}'
                    and account_id in (select {backtest_table_phone_name} from {backtest_table_name})
        """)
    else: 
        sub_df = utils.spark_read_data_from_singlestore(spark, f"""
            select * from {raw_table_name} where s3_file_date > '{activate_from_str}' and s3_file_date <= '{activate_to_str}'
        """)

    max_date = sub_df.groupBy('account_id').agg(F.max("s3_file_date").alias("max_date"))

    service_type_str = """
        CASE 
            WHEN service_class_id IN (101, 300, 201, 369) THEN 'itelecom'
            WHEN service_class_id IN (1369, 1370, 1371, 1372, 1373, 1374, 1375, 1376, 
                        1377, 1378, 1379, 1380, 1381, 1382, 1383, 1384, 
                        1385, 1386, 1387, 1388, 1389, 1629, 1774, 1788, 
                        2229, 2688, 2689) THEN 'prepaid'
            WHEN service_class_id IN (8001, 8885, 449, 999, 8889, 8886, 9001, 8888, 8887) THEN 'test'
            WHEN service_class_id IN (993, 994, 997, 592, 593, 599) THEN 'gtel'
            WHEN service_class_id = 601 THEN 'gmobile'
            WHEN service_class_id in (100, 200) THEN 'post_paid'
            ELSE 'unknown'
        END
    """
    sub_activate = (
        sub_df.join(max_date, "account_id").where("s3_file_date = max_date")\
            .groupBy("account_id")\
            .agg(
                 F.max('account_activated_date').alias('sub_activated_date'),
                 F.max('account_activated_flag').alias('sub_activated_flag'),
                 F.max('service_class_id').alias('service_class_id')
            )
            .withColumnRenamed('account_id', 'msisdn')\
            .withColumn('sub_service_type', F.expr(service_type_str))\
            .withColumn("sub_activate_year", 
                        (F.datediff(F.to_date(F.lit(snapshot_str), "yyyyMMdd"), F.to_date(F.col("sub_activated_date"), "yyyy-MM-dd"))) / 365
                    )
    )
        
    # load lxw fts
#     l1w = spark.read.parquet(in_dir + "/l1w").where(f"date='{snapshot_str}'").drop("date")
#     l4w = spark.read.parquet(in_dir + "/l4w").where(f"date='{snapshot_str}'").drop("date")
#     l12w = spark.read.parquet(in_dir + "/l12w").where(f"date='{snapshot_str}'").drop("date")
#     l24w = spark.read.parquet(in_dir + "/l24w").where(f"date='{snapshot_str}'").drop("date")
    
    l1w = utils.load_from_s3(spark, "sub" , "l1w", run_mode).where(f"date='{snapshot_str}'").drop("date")
    l4w = utils.load_from_s3(spark, "sub" , "l4w", run_mode).where(f"date='{snapshot_str}'").drop("date")
    l12w = utils.load_from_s3(spark, "sub" , "l12w", run_mode).where(f"date='{snapshot_str}'").drop("date")
    l24w = utils.load_from_s3(spark, "sub" , "l24w", run_mode).where(f"date='{snapshot_str}'").drop("date")

    df_fts = l1w.join(l4w, on='msisdn', how='outer')\
                .join(l12w, on='msisdn', how='outer')\
                .join(l24w, on='msisdn', how='outer')
    
    # Create ratio features
    fts_names = [col for col in l1w.columns if col not in ['msisdn', 'date']]
    lxw_list = ['l1w', 'l4w', 'l12w', 'l24w']

    for ft in fts_names:
        for i in range(0, len(lxw_list)):
            for j in range(i+1, len(lxw_list)):
                lxw = lxw_list[i]
                lyw = lxw_list[j]
                new_ft = ft[:ft.rfind('_')] + '_' + lxw + '_vs_' + lyw
                lxw_ft = ft[:ft.rfind('_')] + '_' + lxw
                lyw_ft = ft[:ft.rfind('_')] + '_' + lyw
                df_fts = df_fts.withColumn(new_ft, F.expr(f"{lxw_ft} / {lyw_ft}"))

    df_fts = df_fts.join(sub_activate, 'msisdn', how='outer')
    
    # write to parquet
    target_file_name = f"final_fts/date={snapshot_str}"
    utils.save_to_s3(df_fts, 'sub' , target_file_name, run_mode)
    
    
def process_table(spark, table_name, ALL_MONTHS, fix_date, run_mode):
    if run_mode == "prod": 
        snapshot_str = ALL_MONTHS[0] + fix_date[0]
    
        ### run
        gen_sub_fts_lxw(spark, table_name, snapshot_str, run_mode)
        merge_sub_fts_final(spark, table_name, snapshot_str)

    else:
        for MONTH in ALL_MONTHS:
            for _fix_date in fix_date:
                snapshot_str = MONTH + _fix_date

                ### run
                gen_sub_fts_lxw(spark, table_name, snapshot_str, run_mode)
                merge_sub_fts_final(spark, table_name, snapshot_str)

###################
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

#### GET argument  ####
ALL_MONTHS = config.ALL_MONTHS
ALL_MONTHS.sort(reverse = True)
fix_date = config.fix_date
run_mode = config.run_mode

# create Spark session
spark = utils.create_spark_instance(run_mode)
table_name = 'blueinfo_ocs_sdp_subscriber'
table_name_on_production = config.table_dict[table_name] ### add
backtest_table_name = config.backtest_table_dict[table_name]['backtest_table_name']
backtest_table_phone_name = config.backtest_table_dict[table_name]['backtest_table_phone_name']
    
process_table(spark, table_name_on_production, ALL_MONTHS, fix_date, run_mode)
