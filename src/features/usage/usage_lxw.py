import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

@status_check
def gen_usage_lxw_fts(spark, out_dir, run_date, level):
    print("generating lxw fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")
    sub_group = ["total", "sms", "voice", "voip", "video", "data"]
    
    for sub in sub_group:
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
                    lower_fts = utils.load_from_s3(spark, out_dir + f"/{sub}/daily").where(f"date >= {date_from_str} and date < {date_to_str}")
                    old_suffix = "daily"
                    new_suffix = "l1w"
                else:
                    continue
            else:
                if level != "weekly":
                    lower_fts = utils.load_from_s3(spark, out_dir + f"/{sub}/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                    old_suffix = "l1w"
                    new_suffix = freq_str
                else:
                    continue

            print(f"-- sub group {sub} | fts {freq_str} | level = {level}")
            exclude_list = ["msisdn", "date"]
            aggs = []
            fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
            if old_suffix == "daily":
                aggs.extend(
                        [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols] +
                        [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix).replace("sum", "dmin")) for x in fts_cols if "count" not in x] +
                        [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix).replace("sum", "dmax")) for x in fts_cols if "count" not in x]
                    )
            else:
                aggs.extend(
                        [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["sum", "count"])] +
                        [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "dmin" in x] +
                        [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "dmax" in x]
                    )

            lxw_fts = lower_fts.groupBy("msisdn").agg(*aggs)
            
            # generate avg features
            avg_fts = [x for x in lxw_fts.columns if any(sub in x for sub in ["sum"])]
            for ft in avg_fts:
                lxw_fts = lxw_fts.withColumn(ft.replace("sum", "davg"), F.expr(f"{ft}/{i*7}"))
            
            usage_dir = out_dir + f"/{sub}/{freq_str}/date={run_date}"
            utils.save_to_s3(lxw_fts, usage_dir)

@status_check
def merge_usage_fts(spark, out_dir, run_date):
    sub_group = ["total", "sms", "voice", "voip", "video", "data"]
    print("merging lxw fts for", run_date)

    for sub in sub_group:

        l1w = utils.load_from_s3(spark, out_dir + f"{sub}/l1w").where(f"date='{run_date}'").drop("date")
        l4w = utils.load_from_s3(spark, out_dir + f"{sub}/l4w").where(f"date='{run_date}'").drop("date")
        l12w = utils.load_from_s3(spark, out_dir + f"{sub}/l12w").where(f"date='{run_date}'").drop("date")
        l24w = utils.load_from_s3(spark, out_dir + f"{sub}/l24w").where(f"date='{run_date}'").drop("date")

        df_fts = l1w.join(l4w, on=["msisdn"], how='outer')\
                .join(l12w, on=["msisdn"], how='outer')\
                .join(l24w, on=["msisdn"], how='outer')

         # write to parquet
        usage_lxw_dir= f"{out_dir}/{sub}/final_fts/date={run_date}"
        utils.save_to_s3(df_fts, usage_lxw_dir)


if __name__ == "__main__":

    run_create_feature(
        func_and_kwargs=(
            (gen_usage_lxw_fts, {'level':'weekly'}),
            (gen_usage_lxw_fts, {'level':'lxw'}),
            (merge_usage_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'usage',
            'table_name': None,
        }
    )
