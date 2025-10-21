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

@status_check
def gen_location_l4w(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):
    print("generating lxw fts for:", run_date)
    i = 4
    date_to = datetime.strptime(run_date, "%Y%m%d")
    date_from = date_to - relativedelta(days=i*7)   
    date_from_str = date_from.strftime("%Y%m%d")
    date_to_str = date_to.strftime("%Y%m%d")
    freq_str = f"l{i}w"

    query = f"""
        SELECT msisdn, ma_tinh as province, s3_file_date FROM {table_name}
        WHERE 1=1
            AND s3_file_date >= '{date_from_str}'
            AND s3_file_date < '{date_to_str}'
    """
        
    df_location_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
    df_location_raw.count()

    if run_mode == 'prod': 
        df_location_raw = df_location_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    # count distinct location
    c_distinct = (
        df_location_raw
            .groupBy("msisdn")
            .agg(
                F.countDistinct("province").alias(f"location_count_distinct_{freq_str}"),
                F.count("*").alias(f"location_occurrence_count_{freq_str}")
            )
    )

    # top1 location
    _window = Window.partitionBy("msisdn").orderBy(F.desc(f"location_count_{freq_str}"), F.col("province").asc())
    count_by_loc = df_location_raw\
                    .groupBy("msisdn", "province")\
                    .agg(F.count("*").alias(f"location_count_{freq_str}"))

    top1_loc = count_by_loc\
                    .withColumn("rank", F.row_number().over(_window))\
                    .where("rank=1")\
                    .selectExpr("msisdn", f"province location_top1_province_{freq_str}", f"location_count_{freq_str} location_top1_province_count_{freq_str}")

    # entropy
    entropy = count_by_loc\
                .join(c_distinct, "msisdn", "outer")\
                .withColumn("_p", F.expr(f"location_count_{freq_str} / location_occurrence_count_{freq_str}"))\
                .withColumn("entropy_component", F.expr("-_p * log2(_p)"))

    entropy = entropy.groupBy("msisdn").agg(F.sum("entropy_component").alias(f"location_entropy_{freq_str}"))

    # merge
    top1_province_ratio_str = f"location_top1_province_count_{freq_str} / location_occurrence_count_{freq_str}"
    df_fts = c_distinct\
                .join(entropy, "msisdn", "outer")\
                .join(top1_loc, "msisdn", "outer")\
                .withColumn(f"location_top1_province_ratio_{freq_str}", F.expr(top1_province_ratio_str))
    
    # write to parquet
    utils.save_to_s3(df_fts, out_dir + f"/{freq_str}/date={run_date}")

    df_location_raw.unpersist()


if __name__ == "__main__":
    table_name = "blueinfo_ccbs_cv207"

    run_create_feature(
        func_and_kwargs=(
            (gen_location_l4w, {}),
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'location',
            'table_name': table_name,

        }
    )
