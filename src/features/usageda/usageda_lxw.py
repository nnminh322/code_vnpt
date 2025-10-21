import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils
from src.utils.job_quality_utils import status_check, run_create_feature


@status_check
def gen_usageda_lxw_fts(spark, out_dir, level, run_date, **kwargs):
    print("generating lxw fts for:", run_date)
    run_date = datetime.strptime(run_date, "%Y%m%d")

    if (level == "weekly"):
        freq = [1]
        old_suffix = "daily"

    else:
        freq = [4,12,24]
        old_suffix = "l1w"

    for i in freq:
        # get date from / date to / freq_str

        date_from = run_date - relativedelta(days=i*7)   
        date_from = date_from.strftime("%Y%m%d")
        date_to = run_date.strftime("%Y%m%d")
        freq_str = f"l{i}w"

        lower_fts = utils.load_from_s3(spark, out_dir + f"{old_suffix}/").where(f"date >= {date_from} and date < {date_to}")

        print(f"-- fts {freq_str} | level = {level}")
        fts_cols = [x for x in lower_fts.columns if x not in ["msisdn", "date"]]
        aggs = []
        if old_suffix == "daily":
            aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, freq_str)) for x in fts_cols] +
                    [F.min(F.col(x)).alias(x.replace(old_suffix, freq_str).replace("sum", "dmin")) for x in fts_cols if ("count" not in x)] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, freq_str).replace("sum", "dmax")) for x in fts_cols if ("count" not in x)]
                )
        else:
            aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, freq_str)) for x in fts_cols if any(sub in x for sub in ["sum", "count"])] +
                    [F.min(F.col(x)).alias(x.replace(old_suffix, freq_str)) for x in fts_cols if "dmin" in x] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, freq_str)) for x in fts_cols if "dmax" in x]
                )

        lxw_fts = lower_fts.groupBy("msisdn").agg(*aggs)

        # generate avg features
        avg_fts = [x for x in lxw_fts.columns if any(sub in x for sub in ["sum"])]
        for ft in avg_fts:
            lxw_fts = lxw_fts.withColumn(ft.replace("sum", "avg"), F.expr(f"{ft}/{i*7}"))

        usageda_dir = out_dir + f"/{freq_str}/date={date_to}"
        utils.save_to_s3(lxw_fts, usageda_dir)


@status_check
def merge_usageda_fts(spark, out_dir, run_date, **kwargs):
    print("merging lxw fts for", run_date)
    
    df_fts = utils.load_from_s3(spark,out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    lxw_list = ['l1w', 'l4w', 'l12w', 'l24w']
    for i in lxw_list[1:]:
        _tmp = utils.load_from_s3(spark,out_dir + f"/{i}").where(f"date='{run_date}'").drop("date")
        df_fts = df_fts.join(_tmp, on=["msisdn"], how='outer')

    utils.save_to_s3(df_fts, f"{out_dir}/final_fts/date={run_date}")

if __name__ == "__main__":
    table_name = "blueinfo_voice_msc"

    run_create_feature(
        func_and_kwargs=(
            (gen_usageda_lxw_fts, {'level':'weekly'}),
            (gen_usageda_lxw_fts, {'level':'lxw'}),
            (merge_usageda_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'usageda',
            'table_name': None,

        }
    )
    