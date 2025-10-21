from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .utils import feature_store_utils as fts_utils 
from src.utils import common as utils 

def gen_bts_usage_lxw_fts(spark, bts_usage_dir, snapshot_str, level):
    print("generating lxw fts for:", snapshot_str)
    date_to = datetime.strptime(snapshot_str, "%Y%m%d")

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
                lower_fts =fts_utils.load_from_s3(spark,bts_usage_dir + "/daily").where(f"date >= {date_from_str} and date < {date_to_str}")
                old_suffix = "daily"
                new_suffix = "l1w"
            else:
                continue
        else:
            if level != "weekly":
                lower_fts =fts_utils.load_from_s3(spark,bts_usage_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str
            else:
                continue
    
        print(f"--fts {freq_str} with level = {level}")
        exclude_list = ["msisdn", "date", "bts_id"]
        aggs = []
        fts_cols = [x for x in lower_fts.columns if x not in exclude_list]

        if old_suffix == "daily":
            aggs.extend(
                    [
                        F.avg(F.col(x)).alias(x.replace(old_suffix, new_suffix).replace("count", f"count_davg").replace("sum", f"sum_davg")) for x in fts_cols
                    ]
                )

        else:
            fts_cols = [x for x in fts_cols if not any(sub in x for sub in ["davg", "dmin", "dmax", "dstd"])]
        
        aggs.extend(
                [
                    F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols
                ]
            )

        lxw_fts = lower_fts.groupBy("msisdn", "bts_id").agg(*aggs)

        out_dir = bts_usage_dir + f"/{freq_str}/date={snapshot_str}"
        fts_utils.save_to_s3(lxw_fts, out_dir)


def merge_bts_usage_fts(spark, bts_usage_lxw_dir, snapshot_str):
   
    print("merging lxw fts for", snapshot_str)

    l1w =fts_utils.load_from_s3(spark,bts_usage_lxw_dir + "/l1w").where(f"date='{snapshot_str}'").drop("date")
    l4w =fts_utils.load_from_s3(spark,bts_usage_lxw_dir + "/l4w").where(f"date='{snapshot_str}'").drop("date")
    l12w =fts_utils.load_from_s3(spark,bts_usage_lxw_dir + "/l12w").where(f"date='{snapshot_str}'").drop("date")
    l24w =fts_utils.load_from_s3(spark,bts_usage_lxw_dir + "/l24w").where(f"date='{snapshot_str}'").drop("date")

    df_fts = l1w.join(l4w, on=["msisdn", "bts_id"], how='outer')\
            .join(l12w, on=["msisdn", "bts_id"], how='outer')\
            .join(l24w, on=["msisdn", "bts_id"], how='outer')

     # write to parquet
    out_dir = f"{bts_usage_lxw_dir}/final_fts/date={snapshot_str}"
    fts_utils.save_to_s3(df_fts, out_dir)


## main 
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
bts_usage_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_usage")

spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)
while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    if start_date.weekday() == 0:
        gen_bts_usage_lxw_fts(spark, bts_usage_dir, run_date, level="weekly")
        gen_bts_usage_lxw_fts(spark, bts_usage_dir, run_date, level="lxw")
        merge_bts_usage_fts(spark, bts_usage_dir, run_date)

    start_date = start_date + relativedelta(days=1)