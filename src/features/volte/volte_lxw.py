import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


@status_check
def gen_volte_lxw_fts(spark, out_dir, run_date, level):
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
                lower_fts = utils.load_from_s3(spark,out_dir + "/daily/total_features").where(f"date >= {date_from_str} and date < {date_to_str}")
                old_suffix = "daily"
                new_suffix = "l1w"
            else:
                continue
        else:
            if level != "weekly":
                lower_fts = utils.load_from_s3(spark,out_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str
            else:
                continue
    
        print(f"--fts {freq_str} with level = {level}")
        exclude_list = ["msisdn", "date"]
        
        # create features
        aggs = []
        fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
        
        if old_suffix == "daily":
            aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols] +
                    [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix).replace("sum", "dmin")) for x in fts_cols if ("count" not in x)] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix).replace("sum", "dmax")) for x in fts_cols if "count" not in x]
                )
        else:
            aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["sum", "count"])] +
                    [F.min(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "dmin" in x] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "dmax" in x]
                )
            
        call_in_ratio_str = f"volte_call_in_count_{freq_str} / volte_total_call_count_{freq_str}"
        call_out_ratio_str = f"volte_call_out_count_{freq_str} / volte_total_call_count_{freq_str}"
        cf_ratio_str = f"volte_cf_count_{freq_str} / volte_total_call_count_{freq_str}"

        lxw_fts = lower_fts.groupBy("msisdn").agg(*aggs)\
                           .withColumn(f"volte_call_in_ratio_{freq_str}", F.expr(call_in_ratio_str))\
                           .withColumn(f"volte_call_out_ratio_{freq_str}", F.expr(call_out_ratio_str))\
                           .withColumn(f"volte_cf_ratio_{freq_str}", F.expr(cf_ratio_str))
        
        # generate avg features
        avg_fts = [x for x in lxw_fts.columns if any(sub in x for sub in ["sum", "count"])]
        for ft in avg_fts:
            lxw_fts = lxw_fts.withColumn(ft.replace("sum", "davg").replace("count", "davg"), F.expr(f"{ft}/{i*7}"))

        # write parquet
        final_dir = out_dir + f"/{freq_str}/date={run_date}"
        utils.save_to_s3(lxw_fts, final_dir)

@status_check
def merge_volte_fts(spark, out_dir, run_date):
   
    print("merging lxw fts for", run_date)

    l1w = utils.load_from_s3(spark,out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark,out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark,out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark,out_dir + "/l24w").where(f"date='{run_date}'").drop("date")

    df_fts = l1w.join(l4w, on=["msisdn"], how='outer')\
            .join(l12w, on=["msisdn"], how='outer')\
            .join(l24w, on=["msisdn"], how='outer')

    # Create ratio features
    exclude_list = ["msisdn", "date"]

    fts_names = [col for col in l1w.columns if col not in exclude_list]

    lxw_list = ['l1w', 'l4w', 'l12w', 'l24w']

    for ft in fts_names:
        for i in range(0, len(lxw_list)):
            for j in range(i+2, len(lxw_list)):
                lxw = lxw_list[i]
                lyw = lxw_list[j]
                new_ft = ft[:ft.rfind('_')] + '_' + lxw + '_vs_' + lyw
                lxw_ft = ft[:ft.rfind('_')] + '_' + lxw
                lyw_ft = ft[:ft.rfind('_')] + '_' + lyw
                df_fts = df_fts.withColumn(new_ft, F.expr(f"{lxw_ft} / {lyw_ft}"))
    
    # ratio_fts
    for freq_str in ['l1w', 'l4w', 'l12w', 'l24w']:
        call_in_ratio_str = f"volte_call_in_count_{freq_str} / volte_total_call_count_{freq_str}"
        call_out_ratio_str = f"volte_call_out_count_{freq_str} / volte_total_call_count_{freq_str}"
        cf_ratio_str = f"volte_cf_count_{freq_str} / volte_total_call_count_{freq_str}"
        
        df_fts = (
            df_fts.withColumn(f"volte_call_in_ratio_{freq_str}", F.expr(call_in_ratio_str))\
                  .withColumn(f"volte_call_out_ratio_{freq_str}", F.expr(call_out_ratio_str))\
                  .withColumn(f"volte_cf_ratio_{freq_str}", F.expr(cf_ratio_str))
        )

     # write to parquet
    final_dir = f"{out_dir}/final_fts/date={run_date}"
    utils.save_to_s3(df_fts, final_dir)


if __name__ == "__main__":
    table_name = "blueinfo_voice_msc"

    run_create_feature(
        func_and_kwargs=(
            (gen_volte_lxw_fts, {'level':'weekly'}),
            (gen_volte_lxw_fts, {'level':'lxw'}),
            (merge_volte_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'volte',
            'table_name': None,

        }
    )