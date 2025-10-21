from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .utils import feature_store_utils as fts_utils 
from src.utils import common as utils 

def gen_bts_volte_lxw_fts(spark, bts_volte_dir, snapshot_str, level):
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
                lower_fts =fts_utils.load_from_s3(spark,bts_volte_dir + "/daily/total_features").where(f"date >= {date_from_str} and date < {date_to_str}")
                old_suffix = "daily"
                new_suffix = "l1w"
            else:
                continue
        else:
            if level != "weekly":
                lower_fts =fts_utils.load_from_s3(spark,bts_volte_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str
            else:
                continue
    
        print(f"--fts {freq_str} with level = {level}")
        exclude_list = ["msisdn", "date", "bts_id"]
        
        # create features
        aggs = []
        fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
        
        aggs.extend(
                [
                    F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols
                ]
            )

        call_in_ratio_str = f"bts_volte_call_in_count_{freq_str} / bts_volte_total_call_count_{freq_str}"
        call_out_ratio_str = f"bts_volte_call_out_count_{freq_str} / bts_volte_total_call_count_{freq_str}"
        cf_ratio_str = f"bts_volte_cf_count_{freq_str} / bts_volte_total_call_count_{freq_str}"

        lxw_fts = lower_fts.groupBy("msisdn", "bts_id").agg(*aggs)\
                           .withColumn(f"bts_volte_call_in_ratio_{freq_str}", F.expr(call_in_ratio_str))\
                           .withColumn(f"bts_volte_call_out_ratio_{freq_str}", F.expr(call_out_ratio_str))\
                           .withColumn(f"bts_volte_cf_ratio_{freq_str}", F.expr(cf_ratio_str))
        
        # write parquet
        out_dir = bts_volte_dir + f"/{freq_str}/date={snapshot_str}"
        fts_utils.save_to_s3(lxw_fts, out_dir)


def merge_bts_volte_fts(spark, bts_volte_lxw_dir, snapshot_str):
   
    print("merging lxw fts for", snapshot_str)

    l1w =fts_utils.load_from_s3(spark,bts_volte_lxw_dir + "/l1w").where(f"date='{snapshot_str}'").drop("date")
    l4w =fts_utils.load_from_s3(spark,bts_volte_lxw_dir + "/l4w").where(f"date='{snapshot_str}'").drop("date")
    l12w =fts_utils.load_from_s3(spark,bts_volte_lxw_dir + "/l12w").where(f"date='{snapshot_str}'").drop("date")
    l24w =fts_utils.load_from_s3(spark,bts_volte_lxw_dir + "/l24w").where(f"date='{snapshot_str}'").drop("date")

    df_fts = l1w.join(l4w, on=["msisdn", "bts_id"], how='outer')\
            .join(l12w, on=["msisdn", "bts_id"], how='outer')\
            .join(l24w, on=["msisdn", "bts_id"], how='outer')

    # Create ratio features
    exclude_list = ["msisdn", "date", "bts_id"]

    fts_names = [col for col in l1w.columns if col not in exclude_list]
    fts_names = [x for x in fts_names if not any(sub in x for sub in ["davg", "dmin", "dmax", "dstd", "wavg", "wmin", "wmax", "wstd"])]

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
    
#     # select columns
#     ft_cols = [x for x in df_fts.columns if (x == 'msisdn') | ('bts_' in x)]
#     df_fts = df_fts.select(*ft_cols)

     # write to parquet
    out_dir = f"{bts_volte_lxw_dir}/final_fts/date={snapshot_str}"
    fts_utils.save_to_s3(df_fts, out_dir)

## main 
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
bts_volte_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_volte")
start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)

spark = fts_utils.create_spark_instance(run_mode = run_mode)

while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')

    if start_date.weekday() == 0:
        # run volte: l1w, lxw, merge
        gen_bts_volte_lxw_fts(spark, bts_volte_dir, run_date, level="weekly")
        gen_bts_volte_lxw_fts(spark, bts_volte_dir, run_date, level="lxw")
        merge_bts_volte_fts(spark, bts_volte_dir, run_date)

    start_date = start_date + relativedelta(days=1)