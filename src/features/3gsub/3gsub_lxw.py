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
        ELSE 'other' 
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

lst_category = re.findall(pattern, usage_category_map_expr)
lst_tenure = re.findall(pattern, tenure_expr)

# store col and category for gen fts by loop
feature_column_mapping = {
    "usage_category" : lst_category,
    "tenure" : lst_tenure
}


@status_check
def gen_3gsubs_lxw_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config, level):
    print("generating lxw fts for:", run_date)
    date_to = datetime.strptime(run_date, "%Y%m%d")

    for i in [1, 4, 12, 24]:
        # get date from / date to / freq_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        freq_str = f"l{i}w"
        print(f"--fts {freq_str}")

        if i == 1: 
            if level == 'weekly': 
                df_raw = utils.load_from_s3(spark, out_dir + "/daily").where(f"date >= {date_from_str} and date < {date_to_str}").persist()
                df_raw.count()

                base_aggs = [
                    F.count("*").alias(f"3gsub_action_count_{freq_str}"),
                    F.sum(F.when(F.col("action_category") == 'active', 1).otherwise(0)).alias(f"3gsub_active_count_{freq_str}"),
                    F.sum(F.when(F.col("action_category") == 'cancel', 1).otherwise(0)).alias(f"3gsub_cancel_count_{freq_str}")
                ]
                
                df_fts = df_raw.groupBy("msisdn").agg(*base_aggs)

                for column, categories in feature_column_mapping.items():
                    for category in categories: 
                        if category == 'unknown':
                            continue
                        df_filter = df_raw.where(f"'{column}' == '{category}'")
                        aggs = [
                            F.count("*").alias(f"3gsub_{category}_count_{freq_str}"),
                            F.sum(F.when(F.col("action_category") == 'active', 1).otherwise(0)).alias(f"3gsub_{category}_active_count_{freq_str}"),
                            F.sum(F.when(F.col("action_category") == 'cancel', 1).otherwise(0)).alias(f"3gsub_{category}_cancel_count_{freq_str}")
                        ]

                        df_fts_1 = df_filter.groupBy("msisdn").agg(*aggs)
                
                        df_fts = df_fts.join(df_fts_1, on = 'msisdn', how='outer')
            else: 
                continue
        else: 
            if level != "weekly":
                df_raw = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}")
                old_suffix = "l1w"
                new_suffix = freq_str

                # create sum count features
                exclude_list = ["msisdn", "date"]
                aggs = []
                fts_cols = [x for x in df_raw.columns if x not in exclude_list]

                aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["_count_"])]
                )

                df_fts = df_raw.groupBy("msisdn").agg(*aggs)
            else: 
                continue
            
        # add pct for each category
        for column, categories in feature_column_mapping.items():
            for category in categories: 
                if category == 'unknown':
                        continue
                df_fts = df_fts\
                        .withColumn(f"3gsub_{category}_pct_{freq_str}",
                                        F.col(f"3gsub_{category}_count_{freq_str}") / F.col(f"3gsub_action_count_{freq_str}")
                                    )\
                        .withColumn(f"3gsub_{category}_active_pct_{freq_str}",
                                        F.col(f"3gsub_{category}_active_count_{freq_str}") / F.col(f"3gsub_active_count_{freq_str}")
                                    )\
                        .withColumn(f"3gsub_{category}_cancel_pct_{freq_str}",
                                        F.col(f"3gsub_{category}_cancel_count_{freq_str}") / F.col(f"3gsub_cancel_count_{freq_str}")
                                    )

        # convert numerical to double type 
        df_fts = df_fts.select("msisdn", *[F.col(c).cast("double") for c in df_fts.columns if c != "msisdn"])

        df_raw.unpersist()
        utils.save_to_s3(df_fts, f"{out_dir}/{freq_str}/date={run_date}")

@status_check
def merge_3gsub_fts(spark, out_dir, run_date, **kwargs):
    
    print("merging lxw fts for", run_date)

    l1w = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date='{run_date}'").drop("date")
    l4w = utils.load_from_s3(spark, out_dir + "/l4w").where(f"date='{run_date}'").drop("date")
    l12w = utils.load_from_s3(spark, out_dir + "/l12w").where(f"date='{run_date}'").drop("date")
    l24w = utils.load_from_s3(spark, out_dir + "/l24w").where(f"date='{run_date}'").drop("date")
    
    df_fts = l1w.join(l4w, on="msisdn", how='outer')\
            .join(l12w, on="msisdn", how='outer')\
            .join(l24w, on="msisdn", how='outer')
    
    # agg 
    exclude_list = ["msisdn", "date"]

    # Create lxw vs lyw features
    fts_names = [x for x in df_fts.columns if ("l1w" in x)& (x not in exclude_list)&("_min_" not in x)&("_max_" not in x)&("_std_" not in x)&("_avg_" not in x)&("_pct_" not in x)]
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

    table_name = 'blueinfo_ccbs_spi_3g_subs'

    run_create_feature(
        func_and_kwargs=(
            (gen_3gsubs_lxw_fts, {'level':'weekly'}),
            (gen_3gsubs_lxw_fts, {'level':'lxw'}),
            (merge_3gsub_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'table_name': table_name,
            'feature_name': '3gsubs',

        }
    )