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

def gen_bts_air_lxw_fts(spark, air_table_name, bts_air_dir, bts_mapping_dir, snapshot_str):
    print("generating lxw fts for:", snapshot_str)
    date_to = datetime.strptime(snapshot_str, "%Y%m%d")
    first_day_str = snapshot_str[:-2] + "01"
    
    for i in [1, 4, 12, 24]:
        # get date from / date to / freq_str
        date_from = date_to - relativedelta(days=i*7)   
        date_from_str = date_from.strftime("%Y%m%d")
        date_to_str = date_to.strftime("%Y%m%d")
        freq_str = f"l{i}w"
        print(f"--fts {freq_str}")
        
        #### air 
        query = f"""
            SELECT msisdn, reillamount/1000 as reillamount, cell, origintimestamp
            FROM {air_table_name} 
            WHERE 1=1
                AND cell!= ''
                AND s3_file_date >= '{date_from_str}' 
                AND s3_file_date < '{date_to_str}'
        """
        
        df_air_raw = fts_utils.spark_read_data_from_singlestore(spark, query).persist()
        df_air_raw.count()
        
        df_air = (
            df_air_raw
                .withColumn("timestamps", F.to_timestamp(F.col("origintimestamp").cast("string"), "yyyyMMddHHmmss"))
                .withColumn("is_wk", F.expr(is_wk_str))
                .withColumn("hour", F.date_format("timestamps", "hh"))
                .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
                .withColumn("lac", F.split(F.col("cell"), "-")[2])
                .withColumn("cell_id", F.split(F.col("cell"), "-")[3])
        )
        
        # load bts mapping
        bts_mapping = fts_utils.load_from_s3(spark, bts_mapping_dir + f"/date={first_day_str}")
        
        # write features
        air_fts = (
            df_air.join(bts_mapping, ["lac", "cell_id"], "inner")
                  .where("bts_id is not null")
                  .groupBy("msisdn", "bts_id")
                  .agg(
                        F.count("*").alias(f"bts_recharge_count_{freq_str}"),
                        F.sum(F.expr("case when is_wk = True then 1 end")).alias(f"bts_recharge_count_wk_{freq_str}"),
                        F.sum(F.expr("case when is_wk = False then 1 end")).alias(f"bts_recharge_count_wd_{freq_str}"),
                        F.sum(F.expr("case when time_slot_grp = 'dt' then 1 end")).alias(f"bts_recharge_count_dt_{freq_str}"),
                        F.sum(F.expr("case when time_slot_grp = 'nt' then 1 end")).alias(f"bts_recharge_count_nt_{freq_str}"),
                
                        F.sum("reillamount").alias(f"bts_recharge_sum_{freq_str}"),
                        F.sum(F.expr("case when is_wk = True then reillamount end")).alias(f"bts_recharge_sum_wk_{freq_str}"),
                        F.sum(F.expr("case when is_wk = False then reillamount end")).alias(f"bts_recharge_sum_wd_{freq_str}"),
                        F.sum(F.expr("case when time_slot_grp = 'dt' then reillamount end")).alias(f"bts_recharge_sum_dt_{freq_str}"),
                        F.sum(F.expr("case when time_slot_grp = 'nt' then reillamount end")).alias(f"bts_recharge_sum_nt_{freq_str}")
                    )
        )

        # write to parquet
        out_dir = f"{bts_air_dir}/{freq_str}/date={snapshot_str}"
        fts_utils.save_to_s3(air_fts, out_dir)
        df_air_raw.unpersist()


def merge_bts_air_final_fts(spark, bts_air_lxw_dir, snapshot_str):
    
    print("merging lxw fts for", snapshot_str)

    l1w = fts_utils.load_from_s3(spark, bts_air_lxw_dir + "/l1w").where(f"date='{snapshot_str}'").drop("date")
    l4w = fts_utils.load_from_s3(spark, bts_air_lxw_dir + "/l4w").where(f"date='{snapshot_str}'").drop("date")
    l12w = fts_utils.load_from_s3(spark, bts_air_lxw_dir + "/l12w").where(f"date='{snapshot_str}'").drop("date")
    l24w = fts_utils.load_from_s3(spark, bts_air_lxw_dir + "/l24w").where(f"date='{snapshot_str}'").drop("date")
    
    df_fts = l1w.join(l4w, on=["msisdn", "bts_id"], how='outer')\
            .join(l12w, on=["msisdn", "bts_id"], how='outer')\
            .join(l24w, on=["msisdn", "bts_id"], how='outer')

     # write to parquet
    out_dir = f"{bts_air_lxw_dir}/final_fts/date={snapshot_str}"
    fts_utils.save_to_s3(df_fts, out_dir)


## main
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
air_table_name = "blueinfo_ocs_air"
air_table_name_on_production = config['config']['table_name'][air_table_name]

bts_air_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_air")
bts_mapping_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_mapping")


spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)
while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    if start_date.weekday() == 0:
        gen_bts_air_lxw_fts(spark, air_table_name, bts_air_dir, bts_mapping_dir, run_date)
        merge_bts_air_final_fts(spark, bts_air_dir, run_date)
        
    start_date = start_date + relativedelta(days=1)