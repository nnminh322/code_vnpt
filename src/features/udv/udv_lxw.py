import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


@status_check
def gen_udv_lxw_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, level, run_mode, common_config):
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
        print(f"--fts {freq_str} with level = {level}")

        query = f"""
            SELECT msisdn, pack_id, amount, s3_file_date
            FROM {table_name}  
            WHERE 1=1
                AND s3_file_date >= '{date_from_str}' 
                AND s3_file_date < '{date_to_str}'
        """
            
        if (i==1):
            if (level == "weekly"):
                lower_fts = utils.spark_read_data_from_singlestore(spark, query).persist()
                lower_fts.count()

                if run_mode == 'prod': 
                    lower_fts = lower_fts.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
                # create fts
                df_fts = lower_fts.groupBy("msisdn").agg(
                    F.count("*").alias(f"udv_credit_count_{freq_str}"),
                    F.countDistinct("s3_file_date").alias(f"udv_count_days_{freq_str}"),
                    F.countDistinct("pack_id").alias(f"udv_pack_count_{freq_str}"),
                    F.sum("amount").alias(f"udv_amount_sum_{freq_str}"),
                    F.avg("amount").alias(f"udv_amount_avg_{freq_str}"),
                    F.max("amount").alias(f"udv_amount_max_{freq_str}"),
                )
                
                _cols = [x for x in df_fts.columns if x != "msisdn"]
                for col in _cols:
                    df_fts = df_fts.withColumn(col, F.col(col).cast("double"))
                
            else:
                continue
        else:
            if level != "weekly":
                lower_fts = utils.load_from_s3(spark, out_dir + "/l1w").where(f"date > {date_from_str} and date <= {date_to_str}").persist()
                lower_fts.count()
                old_suffix = "l1w"
                new_suffix = freq_str
                    
                # create features
                exclude_list = ["msisdn", "date"]
                aggs = []
                fts_cols = [x for x in lower_fts.columns if x not in exclude_list]
                
                aggs.extend(
                    [F.sum(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if any(sub in x for sub in ["sum", "count"])] +
                    [F.max(F.col(x)).alias(x.replace(old_suffix, new_suffix)) for x in fts_cols if "max" in x]
                )
                
                udv_avg_str = f"udv_amount_sum_{freq_str}/udv_credit_count_{freq_str}"
                
                df_fts = lower_fts.groupBy("msisdn").agg(*aggs)\
                                  .withColumn(f"udv_amount_avg_{freq_str}", F.expr(udv_avg_str))
                
            else:
                continue
      
        # write to parquet
        udv_dir = out_dir + f"/{freq_str}/date={run_date}"
        utils.save_to_s3(df_fts, udv_dir)
        lower_fts.unpersist()


@status_check    
def merge_udv_fts(spark, out_dir, run_date, **kwargs):
   
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

     # write to parquet
    udv_dir = f"{out_dir}/final_fts/date={run_date}"
    utils.save_to_s3(df_fts, udv_dir)


if __name__ == "__main__":
    table_name = "blueinfo_vascdr_udv_credit_log"

    run_create_feature(
        func_and_kwargs=(
            (gen_udv_lxw_fts, {'level':'weekly'}),
            (gen_udv_lxw_fts, {'level':'lxw'}),
            (merge_udv_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'udv',
            'table_name': table_name,

        }
    )