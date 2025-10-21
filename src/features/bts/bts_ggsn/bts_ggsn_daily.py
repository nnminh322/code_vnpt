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


time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""
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


time_slot_grp_str = """
   case
        when hour >= 6 and hour < 18 then 'dt'
        else 'nt'
    end  
"""

def gen_bts_ggsn_daily_fts(spark, ggsn_table_name, bts_ggsn_dir, bts_mapping_dir, date_str): 

    freq_str = "daily"
    first_day_str = date_str[:-2] + "01"
    
    print("generating daily fts for:", date_str)
    
    query = f"""SELECT msisdn, DURATION, DATA_VOLUME_DOWNLINK, DATA_VOLUME_UPLINK, 
                       LOCATION_AREA as lac, CELL_ID as cell_id, RECORD_OPENING_TIME, s3_file_date
                FROM {ggsn_table_name}
                WHERE 1=1 
                      AND s3_file_date = '{date_str}'
                      AND LOCATION_AREA != ''
                      AND CELL_ID != ''
            """
    
    df_ggsn_raw = fts_utils.spark_read_data_from_singlestore(spark, query).persist()
    
    _c = df_ggsn_raw.count()
    if _c == 0:
        return
    
    # load bts mapping
    bts_mapping = fts_utils.load_from_s3(spark, bts_mapping_dir + f"/date={first_day_str}").drop("s3_file_date")
    df_ggsn = df_ggsn_raw.join(bts_mapping, ["lac", "cell_id"], "inner")\
                       .where("bts_id is not null")
    ## transform
    df = (
        df_ggsn
        .withColumn("DURATION_MINUTES", F.col("DURATION") / 60)
        .withColumn("DATA_VOLUME_DOWNLINK_MB", F.col("DATA_VOLUME_DOWNLINK") / 1000000)
        .withColumn("DATA_VOLUME_UPLINK_MB", F.col("DATA_VOLUME_UPLINK") / 1000000)
        .withColumn("hour", F.date_format("RECORD_OPENING_TIME", "hh"))
        .withColumn("is_wk", F.expr("case when dayofweek(s3_file_date) in (1, 7) then 1 else 0 end"))
        .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
    )
    
    ## general feature
    aggregations = []
    features_list = ["DURATION", "DATA_VOLUME_DOWNLINK", "DATA_VOLUME_UPLINK"]
    for feature in features_list:
        feature = feature.lower()
        aggregations.extend([
            F.sum(feature).cast(T.DoubleType()).alias(f"bts_ggsn_{feature}_sum_{freq_str}"),
            F.count(feature).cast(T.DoubleType()).alias(f"bts_ggsn_{feature}_count_{freq_str}"),
            F.sum(F.expr(f"case when is_wk = True then {feature} end")).alias(f"bts_ggsn_{feature}_wk_sum_{freq_str}"),
            F.sum(F.expr(f"case when is_wk = False then {feature} end")).alias(f"bts_ggsn_{feature}_wd_sum_{freq_str}"),
            F.sum(F.expr(f"case when time_slot_grp = 'dt' then {feature} end")).alias(f"bts_ggsn_{feature}_dt_sum_{freq_str}"),
            F.sum(F.expr(f"case when time_slot_grp = 'nt' then {feature} end")).alias(f"bts_ggsn_{feature}_nt_sum_{freq_str}"),
        ])

    ## add features for duration field
    aggregations.extend([
            F.sum(F.expr("case when DURATION_MINUTES > 5 then 1 else 0 end")).cast(T.DoubleType()).alias(f"bts_ggsn_duration_count_over_5mins_{freq_str}"), 
            F.sum(F.expr("case when DURATION_MINUTES < 0.5 then 1 else 0 end")).cast(T.DoubleType()).alias(f"bts_ggsn_duration_count_under_30secs_{freq_str}"),
    ])
        
    
    ## add other features (user behavior)
    aggregations.extend([
            F.sum(F.expr("case when is_wk = True then 1 else 0 end")).alias(f"bts_ggsn_wk_count_{freq_str}"),
            F.sum(F.expr("case when is_wk = False then 1 else 0 end")).alias(f"bts_ggsn_wd_count_{freq_str}"),
            F.sum(F.expr("case when DATA_VOLUME_DOWNLINK_MB = 0 then 0 else 1 end")).alias(f"bts_ggsn_downlink_count_{freq_str}"),
            F.sum(F.expr("case when DATA_VOLUME_UPLINK_MB = 0 then 0 else 1 end")).alias(f"bts_ggsn_uplink_count_{freq_str}")
    ])

    df_fts = df.groupBy("msisdn", "bts_id").agg(*aggregations)
    
    path = bts_ggsn_dir + f'/{freq_str}/date={date_str}'
    fts_utils.save_to_s3(df_fts, path)
    
    df_ggsn_raw.unpersist()
    

## main
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
ggsn_table_name = 'blueinfo_ggsn' 
ggsn_table_name_on_production = config['config']['table_name'][ggsn_table_name]

bts_ggsn_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_ggsn")
bts_mapping_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_mapping")

spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)
while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    # bts ggsn daily
    gen_bts_ggsn_daily_fts(spark, ggsn_table_name, bts_ggsn_dir, bts_mapping_dir, run_date)

    start_date = start_date + relativedelta(days=1)
