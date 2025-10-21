import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature


@status_check
def gen_money_transfer_lxw(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, level, run_mode, common_config):
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

        # query not depend on run_mode
        query = f"""
            SELECT SENDER, AMOUNT, RECEIVER, s3_file_date
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
                    lower_fts = lower_fts.where("left(SENDER, 4) = '0084' and length(SENDER) = 13")
     
                # create fts
                sender_fts = lower_fts.groupBy("SENDER").agg(
                    F.count("*").alias(f"money_transfer_sender_count_{freq_str}"),
                    F.countDistinct("s3_file_date").alias(f"money_transfer_sender_count_days_{freq_str}"),
                    F.sum("AMOUNT").alias(f"money_transfer_sender_amount_sum_{freq_str}"),
                    F.avg("AMOUNT").alias(f"money_transfer_sender_amount_avg_{freq_str}"),
                    F.max("AMOUNT").alias(f"money_transfer_sender_amount_max_{freq_str}"),
                )\
                .withColumnRenamed("SENDER", "msisdn")

                receiver_fts = lower_fts.groupBy("RECEIVER").agg(
                    F.count("*").alias(f"money_transfer_receiver_count_{freq_str}"),
                    F.countDistinct("s3_file_date").alias(f"money_transfer_receiver_count_days_{freq_str}"),
                    F.sum("AMOUNT").alias(f"money_transfer_receiver_amount_sum_{freq_str}"),
                    F.avg("AMOUNT").alias(f"money_transfer_receiver_amount_avg_{freq_str}"),
                    F.max("AMOUNT").alias(f"money_transfer_receiver_amount_max_{freq_str}"),
                )\
                .withColumnRenamed("RECEIVER", "msisdn")

                money_transfer_sum_str = f"money_transfer_sender_amount_sum_{freq_str} + money_transfer_receiver_amount_sum_{freq_str}"
                df_fts = (
                    sender_fts.join(receiver_fts, "msisdn", "outer")
                              .withColumn(f"money_transfer_amount_sum_{freq_str}", F.expr(money_transfer_sum_str))
                )

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
                
                sender_amount_avg_str = f"money_transfer_sender_amount_sum_{freq_str}/money_transfer_sender_count_{freq_str}"
                receiver_amount_avg_str = f"money_transfer_receiver_amount_sum_{freq_str}/money_transfer_receiver_count_{freq_str}"
                
                df_fts = lower_fts.groupBy("msisdn").agg(*aggs)\
                                  .withColumn(f"money_transfer_sender_amount_avg_{freq_str}", F.expr(sender_amount_avg_str))\
                                  .withColumn(f"money_transfer_receiver_amount_avg_{freq_str}", F.expr(receiver_amount_avg_str))
                
            else:
                continue
      
        # write to parquet
        utils.save_to_s3(df_fts, out_dir + f"/{freq_str}/date={run_date}")
        lower_fts.unpersist()
        
@status_check       
def merge_money_transfer_fts(spark, out_dir, run_date, **kwargs):
   
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
    utils.save_to_s3(df_fts, f"{out_dir}/final_fts/date={run_date}")


if __name__ == "__main__":
    table_name = "blueinfo_vascdr_2friend_log"

    run_create_feature(
        func_and_kwargs=(
            (gen_money_transfer_lxw, {'level':'weekly'}),
            (gen_money_transfer_lxw, {'level':'lxw'}),
            (merge_money_transfer_fts, {})
        ),
        global_kwargs={
            'freq': 'weekly',
            'feature_name': 'money_transfer',
            'table_name': table_name,

        }
    )