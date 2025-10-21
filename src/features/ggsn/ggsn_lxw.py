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

@status_check
def gen_ggsn_lxw_fts(spark, run_date, out_dir, level):
    print("generating lxw fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")

    # if level = weekly then generate l1w features
    # else generate lxw features --> difference directory

    for i in [1, 4, 12, 24]:
        # get date from / date to / freq_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        freq_str = f"l{i}w"

        if (i==1):
            if (level == "weekly"):
                lower_fts = utils.load_from_s3(spark, out_dir + "/daily").where(f"date >= {date_from_str} and date < {date_to_str}")
                old_suffix = "daily"
                new_suffix = "l1w"
            else:
                continue
        else:
            if level != "weekly":
                lower_fts = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date >= {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str
            else:
                continue

        print(f"--fts {freq_str} with level = {level}")
        exclude_list = ["msisdn", "date"]
        aggs = []
        fts_cols = [x for x in lower_fts.columns if x not in exclude_list]


        aggs.extend(
            [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if '_min_' in x] + 
            [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if '_max_' in x] + 
            [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(char in x for char in ['_count_', '_sum_']) and '_ratio_' not in x]
            )
    

        ## add ratio features for duration field 
        aggs.extend([
            (F.sum(f"ggsn_duration_count_over_5mins_{old_suffix}")/F.sum(f"ggsn_duration_count_{old_suffix}")).alias(f"ggsn_duration_count_ratio_over_5mins_{freq_str}"),
            (F.sum(f"ggsn_duration_count_under_30secs_{old_suffix}")/F.sum(f"ggsn_duration_count_{old_suffix}")).alias(f"ggsn_duration_count_ratio_under_30secs_{freq_str}"),
        ])        
    
        lxw_fts = lower_fts.groupBy("msisdn").agg(*aggs)
        utils.save_to_s3(lxw_fts, out_dir + f"/{freq_str}/date={run_date}")

@status_check
def merge_ggsn_fts(spark, out_dir, run_date):
   
    print("merging lxw fts for", run_date)

    l1w = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark, out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark, out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark, out_dir + "/l24w").where(f"date='{run_date}'").drop("date")
    
    df_fts = l1w.join(l4w, on=["msisdn"], how='outer')\
            .join(l12w, on=["msisdn"], how='outer')\
            .join(l24w, on=["msisdn"], how='outer')

    # Create ratio features
    exclude_list = ["msisdn", "date"]

    fts_names = [col for col in l1w.columns if col not in exclude_list]
    fts_names = [x for x in fts_names if not any(sub in x for sub in ["davg", "dmin", "dmax", "dstd", "wavg", "wmin", "wmax", "wstd"])]

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

     # write to parquet
    utils.save_to_s3(df_fts, f"{out_dir}/final_fts/date={run_date}")

if __name__ == "__main__":
    table_name = "blueinfo_ggsn"

    run_create_feature(
        func_and_kwargs=(
            (gen_ggsn_lxw_fts, {'level':'weekly'}),
            (gen_ggsn_lxw_fts, {'level':'lxw'}),
            (merge_ggsn_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'ggsn',
            'table_name': None,

        }
    )
