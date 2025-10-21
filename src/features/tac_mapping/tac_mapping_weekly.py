from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
import pandas as pd
import numpy as np
import time
import sys
from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

import os
os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

@status_check
def gen_tac_mapping_weekly(spark, run_date, out_dir): 
    window = Window.partitionBy("msisdn").orderBy(F.col("_c").desc())

    print("generating weekly fts for:", run_date)

    date_to = datetime.strptime(run_date, "%Y%m%d")
    date_from = date_to - relativedelta(months=1)

    # convert datetime to string
    date_from_str = date_from.strftime("%Y%m%d")
    date_to_str = date_to.strftime("%Y%m%d")
    
    # load data and transform 
    df = utils.load_from_s3(spark, out_dir + "/daily").where(f"date >= {date_from_str} and date < {date_to_str}") # columsn: msisdn, tac, date
    df_count = df.groupBy("msisdn", "tac").agg(F.count("*").alias("_c"))

    # calculate
    df_rank = df_count.withColumn("rank", F.row_number().over(window))
    result = df_rank.filter(F.col("rank") == 1).select("msisdn", "tac")
    
    # save data
    mapping_out_dir = f"{out_dir}/weekly/date={run_date}"
    utils.save_to_s3(result, mapping_out_dir)

if __name__ == "__main__":

    table_name = 'blueinfo_ocs_crs_usage'

    run_create_feature(
        func_and_kwargs=(
            (gen_tac_mapping_weekly, {}),
        ),
        global_kwargs={
            'freq': 'weekly',
            'table_name': None,
            'feature_name': 'tac_mapping',

        }
    ) 