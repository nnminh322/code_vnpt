from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .utils import feature_store_utils as fts_utils 
from src.utils import common as utils 

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


def gen_bts_voice_daily_fts(spark, voice_table_name, bts_voice_dir, bts_mapping_dir, date_str):

    freq_str = "daily"
    first_day_str = date_str[:-2] + "01"
    
    print("generating daily fts for:", date_str)
    
    call_type = {
        "call_in": "MTC",
        "call_out": "MOC",
#         "roam": "ROA",
#         "cf": "CF"
    }
    
    sms_type = {
        "sms_in": "SMT",
        "sms_out": "SMO"
    }
    
    super_dict = {
        "call": call_type,
        "sms": sms_type
    }
    
    for sub_group, _dict in super_dict.items():
        for _type, _filter in _dict.items():
            
            # load data
            query = f"""
                SELECT MSISDN, CALL_DURATION, CALL_TYPE, 
                       LOCATION_AREA as lac, CELL_ID as cell_id,
                       s3_file_date
                FROM {voice_table_name} 
                WHERE 1=1
                    AND ((LOCATION_AREA != '') AND (CELL_ID != ''))
                    AND CALL_TYPE = '{_filter}'
                    AND s3_file_date = '{date_str}'
            """

            df_voice_raw =fts_utils.spark_read_data_from_singlestore(spark, query).persist()
            _c = df_voice_raw.count()

            if _c == 0:
                print(sub_group, _type, _filter)
                print(query)
                print("-"*100)
                continue

            df_voice = (
                df_voice_raw
                    .withColumnRenamed("MSISDN", "msisdn")
                    .withColumn("timestamps", F.to_date("s3_file_date", "yyyyMMdd"))
                    .withColumn("is_wk", F.expr(is_wk_str))
            )

            # merge with bts mapping
            bts_mapping =fts_utils.load_from_s3(spark,bts_mapping_dir + f"/date={first_day_str}")
            df_voice = df_voice.join(bts_mapping, ["lac", "cell_id"], "inner")\
                               .where("bts_id is not null")
            
            # create features
            aggs = [
                F.count("*").alias(f"bts_voice_{_type}_count_{freq_str}"),
                F.sum(F.expr("case when is_wk = True then 1 end")).alias(f"bts_voice_{_type}_count_wk_{freq_str}"),
                F.sum(F.expr("case when is_wk = False then 1 end")).alias(f"bts_voice_{_type}_count_wd_{freq_str}")                
            ]
            
            if sub_group == "call":
                aggs.extend(
                    [
                        F.sum("CALL_DURATION").alias(f"bts_voice_{_type}_duration_sum_{freq_str}"),
                        F.sum(F.expr("case when is_wk = True then CALL_DURATION end")).alias(f"bts_voice_{_type}_duration_sum_wk_{freq_str}"),
                        F.sum(F.expr("case when is_wk = False then CALL_DURATION end")).alias(f"bts_voice_{_type}_duration_sum_wd_{freq_str}") 
                    ]
                
                )

            df_voice = df_voice.groupBy("msisdn", "bts_id").agg(*aggs)
            
            out_dir = bts_voice_dir + f"/{freq_str}/{sub_group}/{_type}/date={date_str}"
            fts_utils.save_to_s3(df_voice, out_dir)

    # load saved features
    dfs = []
    for sub_group, _dict in super_dict.items():
        for _type, _filter in _dict.items():
            out_dir = bts_voice_dir + f"/{freq_str}/{sub_group}/{_type}/date={date_str}"
            
            try:
                df =fts_utils.load_from_s3(spark,out_dir)
                dfs.append(df)
            except:
                print("skip")
    df_fts = dfs[0]

    for _df in dfs[1:]:
        df_fts = df_fts.join(_df, ["msisdn", "bts_id"], how="outer")
    
    # calculate more features
    total_call_str = " + ".join([f"bts_voice_{_type}_count_{freq_str}" for _type in call_type.keys()])
    total_call_wk_str = " + ".join([f"bts_voice_{_type}_count_wk_{freq_str}" for _type in call_type.keys()])
    total_call_wd_str = " + ".join([f"bts_voice_{_type}_count_wd_{freq_str}" for _type in call_type.keys()])

    total_sms_str = " + ".join([f"bts_voice_{_type}_count_{freq_str}" for _type in sms_type.keys()])
    total_sms_wk_str = " + ".join([f"bts_voice_{_type}_count_wk_{freq_str}" for _type in sms_type.keys()])
    total_sms_wd_str = " + ".join([f"bts_voice_{_type}_count_wd_{freq_str}" for _type in sms_type.keys()])


    df_fts = df_fts.withColumn(f"bts_voice_total_call_count_{freq_str}", F.expr(total_call_str))\
                   .withColumn(f"bts_voice_total_call_wk_count_{freq_str}", F.expr(total_call_wk_str))\
                   .withColumn(f"bts_voice_total_call_wd_count_{freq_str}", F.expr(total_call_wd_str))\
                   .withColumn(f"bts_voice_total_sms_count_{freq_str}", F.expr(total_sms_str))\
                   .withColumn(f"bts_voice_total_sms_wk_count_{freq_str}", F.expr(total_sms_wk_str))\
                   .withColumn(f"bts_voice_total_sms_wd_count_{freq_str}", F.expr(total_sms_wd_str))\
                   .withColumn(f"bts_voice_total_count_{freq_str}", F.expr(f"bts_voice_total_call_count_{freq_str} + bts_voice_total_sms_count_{freq_str}"))
    
    # save to parquet
    final_dir = bts_voice_dir + f"/{freq_str}/total_features/date={date_str}"
    fts_utils.save_to_s3(df_fts, final_dir)

    df_voice_raw.unpersist()


## main 
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
voice_table_name = "blueinfo_voice_msc"
voice_table_name_on_production = config['config']['table_name'][voice_table_name]
bts_voice_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_voice")
bts_mapping_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_mapping")

spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)
while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    gen_bts_voice_daily_fts(spark, voice_table_name, bts_voice_dir, bts_mapping_dir, run_date)

    start_date = start_date + relativedelta(days=1)