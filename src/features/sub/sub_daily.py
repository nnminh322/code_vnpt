import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


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

@status_check
def gen_sub_fts_daily(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):
    # create lxw fts
    print("generating daily fts for:", run_date)
        
    # get date from / date to / lxw_str
    lxw_str = f"daily"
    
    query = f"""
        SELECT account_id, account_balance, service_class_id,
                account_activated_date, account_activated_flag
        FROM {table_name}
        WHERE s3_file_date = '{run_date}'    
    """

    sub = utils.spark_read_data_from_singlestore(spark, query).persist()
    sub.count()

    if run_mode == 'prod': 
        sub = sub.where("left(account_id, 4) = '0084' and length(account_id) = 13")
     
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

    sub = sub.withColumnRenamed('account_id', 'msisdn')
    
    # write to parquet
    out_dir = f"{out_dir}/{lxw_str}/date={run_date}"
    utils.save_to_s3(sub, out_dir)
    sub.unpersist()


if __name__ == "__main__":

    table_name = 'blueinfo_ocs_sdp_subscriber'

    run_create_feature(
        func_and_kwargs=(
            (gen_sub_fts_daily, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'sub',

        }
    )