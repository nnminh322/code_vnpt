import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

is_wk_str = "dayofweek(timestamps) in (1, 7)"
time_slot_str = """
    case
        when (hour < 6) or (hour >= 18) then 'nt'
        when (hour >= 6) and (hour < 12) then 'mo'
        when (hour >= 12) and (hour < 18) then 'af_ev'
    end
"""

time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""

@status_check
def gen_volte_daily_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config, table_name_v2):

    freq_str = "daily"
    print("generating daily fts for:", run_date)
    
    call_type = {
        "call_in": "MTC",
        "call_out": "MOC",
        "cf": "CF"
    }
    
    for _type, _filter in call_type.items():

        if run_date < "20250301": 
            query = f"""
                SELECT a_subs as msisdn, call_type, call_duration,
                    s3_file_date
                FROM {table_name} 
                WHERE 1=1
                    AND CALL_TYPE = '{_filter}'
                    AND s3_file_date = '{run_date}'
            """
        else:
            _config = utils.load_config("config_edit", "to_edit.yaml")
            table_v2 = _config['table_name'][table_name_v2]
            query = f"""
                SELECT a_subs as msisdn, call_type, call_duration,
                    s3_file_date
                FROM {table_v2} 
                WHERE 1=1
                    AND CALL_TYPE = '{_filter}'
                    AND s3_file_date = '{run_date}'
            """

        df_volte_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
        df_volte_raw.count()

        if run_mode == 'prod': 
            df_volte_raw = df_volte_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")


        df_volte = (
            df_volte_raw
                .withColumnRenamed("MSISDN", "msisdn")
                .withColumn("timestamps", F.to_date("s3_file_date", "yyyyMMdd"))
                .withColumn("is_wk", F.expr(is_wk_str))
        )
        
        # create features
        aggs = [
            F.count("*").alias(f"volte_{_type}_count_{freq_str}"),
            F.sum(F.expr("case when is_wk = True then 1 end")).alias(f"volte_{_type}_count_wk_{freq_str}"),
            F.sum(F.expr("case when is_wk = False then 1 end")).alias(f"volte_{_type}_count_wd_{freq_str}")                
        ]
        
        aggs.extend(
            [
                F.sum("CALL_DURATION").alias(f"volte_{_type}_duration_sum_{freq_str}"),
                F.sum(F.expr("case when is_wk = True then CALL_DURATION end")).alias(f"volte_{_type}_duration_sum_wk_{freq_str}"),
                F.sum(F.expr("case when is_wk = False then CALL_DURATION end")).alias(f"volte_{_type}_duration_sum_wd_{freq_str}") 
            ]
        
        )

        df_volte = df_volte.groupBy("msisdn").agg(*aggs)
        
        volte_dir = out_dir + f"/{freq_str}/{_type}/date={run_date}"
        utils.save_to_s3(df_volte, volte_dir)

    # load saved features
    dfs = []
    for _type, _filter in call_type.items():
        volte_dir = out_dir + f"/{freq_str}/{_type}/date={run_date}"
        
        try:
            df = utils.load_from_s3(spark,volte_dir)
            dfs.append(df)
        except:
            print("skip")
    df_fts = dfs[0]

    for _df in dfs[1:]:
        df_fts = df_fts.join(_df, ["msisdn"], how="outer")
    
    # calculate more features
    total_call_str = " + ".join([f"volte_{_type}_count_{freq_str}" for _type in call_type.keys()])
    total_call_wk_str = " + ".join([f"volte_{_type}_count_wk_{freq_str}" for _type in call_type.keys()])
    total_call_wd_str = " + ".join([f"volte_{_type}_count_wd_{freq_str}" for _type in call_type.keys()])

    df_fts = df_fts.withColumn(f"volte_total_call_count_{freq_str}", F.expr(total_call_str))\
                   .withColumn(f"volte_total_call_wk_count_{freq_str}", F.expr(total_call_wk_str))\
                   .withColumn(f"volte_total_call_wd_count_{freq_str}", F.expr(total_call_wd_str))
    
    # save to parquet
    final_dir = out_dir + f"/{freq_str}/total_features/date={run_date}"
    utils.save_to_s3(df_fts, final_dir)

    df_volte_raw.unpersist()

if __name__ == "__main__":
    table_name = "blueinfo_voice_volte"
    table_name_v2 = "blueinfo_voice_volte_v2"

    run_create_feature(
        func_and_kwargs=(
            (gen_volte_daily_fts, {"table_name_v2" : table_name_v2}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'volte',

        }
    )
