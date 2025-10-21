import re
import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

pattern = r"(?i)(?:then|else)\s+'([^']+)'"
specific_timestamp = "1970-01-01 00:00:00"

usage_category_map_expr = '''
    CASE 
        WHEN service_id like 'gd_%' or service_id like '%home%' THEN 'family'
        WHEN service_id like 'vp%' THEN 'office'
        WHEN service_id like 'wifi_offload%' THEN 'public'
        WHEN service_id like 'gt%' or service_id like 'mytv%' THEN 'relax'
        ELSE 'other_purpose' 
    END
'''

tenure_expr = '''
    CASE 
        WHEN package_duration_days < 0 then 'unknown'
        WHEN package_duration_days <= 1 then 'one_day'
        WHEN package_duration_days <= 7 then 'few_days'
        WHEN package_duration_days <= 31 then 'few_weeks'
        WHEN package_duration_days <= 180 then 'few_months'
        ELSE 'long_term' 
    END
'''

last_action_category_expr = '''
    CASE
        WHEN last_action in ('CREATE', 'EXTEND') then 'active'
        WHEN last_action in ('DELETE') THEN 'cancel'
        ELSE 'other'
    END
'''

@status_check
def gen_3gsubs_daily_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):
    print("generating daily fts for:", run_date)
    freq_str = 'daily'

    query = f"""
        SELECT msisdn, time_start, time_end, service_id, last_action, id
        FROM {table_name}
        WHERE 1=1
            AND s3_file_date = '{run_date}'    
    """

    df_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
    df_raw.count()

    if run_mode == 'prod': 
        df_raw = df_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    df_raw = (
        df_raw
            .withColumn("time_start", F.to_timestamp(F.col("time_start").cast("string"), "yyyyMMddHHmmss"))
            .withColumn("time_end", F.when(F.col("time_end") == 'null'
                                            , F.to_timestamp(F.lit(specific_timestamp), 'yyyyMMddHHmmss'))
                                    .otherwise(F.to_timestamp(F.col("time_end").cast("string"), "yyyyM1MddHHmmss"))
            )
            .withColumn("usage_category", F.expr(usage_category_map_expr))
            .withColumn("package_duration_days", (F.col("time_end").cast("long") - F.col("time_start").cast("long"))/86400)
            .withColumn("tenure", F.expr(tenure_expr))
            .withColumn("action_category", F.expr(last_action_category_expr))
    )

    utils.save_to_s3(df_raw, f"{out_dir}/{freq_str}/date={run_date}")
    df_raw.unpersist()

if __name__ == "__main__":

    table_name = 'blueinfo_ccbs_spi_3g_subs'

    run_create_feature(
        func_and_kwargs=(
            (gen_3gsubs_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': '3gsubs',

        }
    )