import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


@status_check
def gen_sub_fts_lxw(spark, out_dir, run_date):
    # create lxw fts
    date_to = datetime.strptime(run_date, "%Y%m%d")
    print("generating lxw fts for:", run_date)
    for i in [1, 4, 12, 24]:
        # get date from / date to / lxw_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        lxw_str = f"l{i}w"
        print(f"--fts {lxw_str}")

        sub_daily = utils.load_from_s3(spark, f"{out_dir}/daily")\
                         .where(f"date >= '{date_from_str}' and date < '{date_to_str}'")

        # write fts
        df_fts = sub_daily.groupby("msisdn")\
                    .agg(
                         F.avg("account_balance").alias(f"sub_balance_avg_per_day_{lxw_str}"),
                         F.max("account_balance").alias(f"sub_balance_max_per_day_{lxw_str}"),
                         F.stddev("account_balance").alias(f"sub_balance_std_per_day_{lxw_str}"),
                         F.mean("lt_1k").alias(f"sub_balance_pct_day_lt_1k_{lxw_str}"),
                         F.mean("lt_5k").alias(f"sub_balance_pct_day_lt_5k_{lxw_str}"),
                         F.mean("lt_10k").alias(f"sub_balance_pct_day_lt_10k_{lxw_str}"),
                         F.mean("is_zero").alias(f"sub_balance_pct_day_zero_{lxw_str}"),
                         F.mean("ge_1k_lt_5k").alias(f"sub_balance_pct_day_1k_to_5k_{lxw_str}"),
                         F.mean("ge_5k_lt_10k").alias(f"sub_balance_pct_day_5k_to_10k_{lxw_str}"),
                         F.mean("ge_10k_lt_20k").alias(f"sub_balance_pct_day_10k_to_20k_{lxw_str}"),
                         F.mean("ge_20k_lt_50k").alias(f"sub_balance_pct_day_20k_to_50k_{lxw_str}"),
                         F.mean("ge_50k_lt_100k").alias(f"sub_balance_pct_day_50k_to_100k_{lxw_str}"),
                         F.mean("ge_100k").alias(f"sub_balance_pct_day_ge_100k_{lxw_str}")
                    )                             
            
        # write to parquet
        sub_dir = f"{out_dir}/{lxw_str}/date={run_date}"
        utils.save_to_s3(df_fts, sub_dir)

@status_check
def merge_sub_fts(spark, out_dir, run_date):
    
    print("merging lxw fts for", run_date)

    # Get activate date in last 30d
    activate_to = datetime.strptime(run_date, "%Y%m%d")
    activate_from = activate_to - relativedelta(days=30)
    activate_to_str = activate_to.strftime("%Y%m%d")
    activate_from_str = activate_from.strftime("%Y%m%d")

    sub_df = utils.load_from_s3(spark, f"{out_dir}/daily")\
                            .where(f"date >= '{activate_from_str}' and date < '{activate_to_str}'")

    max_date = sub_df.groupBy('msisdn').agg(F.max("date").alias("max_date"))

    service_type_str = """
        CASE 
            WHEN service_class_id IN (101, 300, 201, 369) THEN 'itelecom'
            WHEN service_class_id IN (1369, 1370, 1371, 1372, 1373, 1374, 1375, 1376, 
                        1377, 1378, 1379, 1380, 1381, 1382, 1383, 1384, 
                        1385, 1386, 1387, 1388, 1389, 1629, 1774, 1788, 
                        2229, 2688, 2689) THEN 'prepaid'
            WHEN service_class_id IN (8001, 8885, 449, 999, 8889, 8886, 9001, 8888, 8887) THEN 'test'
            WHEN service_class_id IN (993, 994, 997, 592, 593, 599) THEN 'gtel'
            WHEN service_class_id = 601 THEN 'gmobile'
            WHEN service_class_id in (100, 200) THEN 'post_paid'
            ELSE 'unknown'
        END
    """
    sub_activate = (
        sub_df.join(max_date, "msisdn").where("date = max_date")\
            .groupBy("msisdn")\
            .agg(
                 F.max('account_activated_date').alias('sub_activated_date'),
                 F.max('account_activated_flag').alias('sub_activated_flag'),
                 F.max('service_class_id').alias('service_class_id')
            )
            .withColumn('sub_service_type', F.expr(service_type_str))\
            .withColumn("sub_activate_year", 
                        (F.datediff(F.to_date(F.lit(run_date), "yyyyMMdd"), F.to_date(F.col("sub_activated_date"), "yyyy-MM-dd"))) / 365
                    )
    )
        
    # load lxw fts
    
    l1w = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark, out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark, out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark, out_dir + "/l24w").where(f"date='{run_date}'").drop("date")


    df_fts = l1w.join(l4w, on='msisdn', how='outer')\
                .join(l12w, on='msisdn', how='outer')\
                .join(l24w, on='msisdn', how='outer')
    
    # Create ratio features
    fts_names = [col for col in l1w.columns if col not in ['msisdn', 'date']]
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

    df_fts = df_fts.join(sub_activate, 'msisdn', how='outer')
    
     # write to parquet
    out_dir = f"{out_dir}/final_fts/date={run_date}"
    utils.save_to_s3(df_fts, out_dir)


if __name__ == "__main__":
    table_name = "blueinfo_ocs_sdp_subscriber"

    run_create_feature(
        func_and_kwargs=(
            (gen_sub_fts_lxw, {}),
            (merge_sub_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'sub',
            'table_name': None,

        }
    )
    