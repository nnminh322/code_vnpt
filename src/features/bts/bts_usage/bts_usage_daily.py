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


def gen_bts_usage_daily_fts(spark, usage_table_name, bts_usage_out_dir, bts_mapping_dir, date_str):

    freq_str = "daily"
    first_day_str = date_str[:-2] + "01"
    
    print("generating daily fts for:", date_str)

    # load data
    query = f"""
        SELECT MSISDN, ORIGINALTIMESTAMP, ORIGINATINGCELLID,
            DURATION, VOLUME, TOTALCOST / 1000 as TOTALCOST,
            CHARGINGCATEGORYNAME, SERVICENAME
        FROM {usage_table_name} 
        WHERE 1=1
            AND ORIGINATINGCELLID != ''
            AND s3_file_date = '{date_str}' 
    """
    # load data
    df_usage_raw =fts_utils.spark_read_data_from_singlestore(spark, query).persist()
    _c = df_usage_raw.count()
    
    if _c == 0:
        return

    df_usage = (
        df_usage_raw
            .withColumnRenamed("MSISDN", "msisdn")
            .withColumn("timestamps", F.to_timestamp("ORIGINALTIMESTAMP", "yyyyMMdd'T'HH:mm:ss"))
            .withColumn("is_wk", F.expr(is_wk_str))
            .withColumn("hour", F.date_format("timestamps", "hh"))
            .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
            .withColumn("lac", F.split(F.col("ORIGINATINGCELLID"), "-")[2])
            .withColumn("cell_id", F.split(F.col("ORIGINATINGCELLID"), "-")[3])
    )
    
    # merge with bts mapping
    bts_mapping =fts_utils.load_from_s3(spark,bts_mapping_dir + f"/date={first_day_str}")
    df_usage = df_usage.join(bts_mapping, ["lac", "cell_id"], "inner")\
                       .where("bts_id is not null")
    
    # caclulate features
    # create dicts
    total_dict = {
        '1': 'count',
        'DURATION': 'call_duration_sum',
        'TOTALCOST': 'total_cost_sum'
    }
    sms_dict = {
        'TOTALCOST': 'sms_total_cost_sum'
    }

    voice_dict = {
        'DURATION': 'voice_duration_sum',
        'TOTALCOST': 'voice_total_cost_sum'
    }

    voip_dict = {
        'DURATION': 'voice_voip_duration_sum',
        'TOTALCOST': 'voice_voip_total_cost_sum'
    }

    video_telephony_dict = {
        'DURATION': 'video_duration_sum',
        'TOTALCOST': 'video_total_cost_sum'
    }

    data_dict = {
        'VOLUME': 'data_volume_sum',
        'TOTALCOST': 'data_total_cost_sum'
    }

    super_dict = {
        "total": total_dict,
        "'SMS'": sms_dict,
        "'Voice'": voice_dict,
        "'VOIP'": voip_dict,
        "'Video Telephony'": video_telephony_dict,
        "'MBC', 'Data'": data_dict

    }

    dfs = []
    # for each dict --> calculate features in dict
    for service_name, _dict in super_dict.items():
        if service_name == "'MBC', 'Data'":
            _tmp = "_data"
        elif service_name == "'Video Telephony'":
            _tmp = "_video"
        elif service_name == "total":
            _tmp = ""
        else:
            _tmp = "_" + service_name.replace("'", "").lower()

        aggs = [F.count("*").alias(f"bts_usage{_tmp.lower()}_count_{freq_str}")]

        # create mutilple agg then group by
        for k, v in _dict.items():
            if k != '1':
                aggs.extend([F.sum(F.col(k)).alias(f"bts_usage_{v}_{freq_str}")])
                
            aggs.extend(
                [
                    F.sum(F.expr(f"case when is_wk = True then {k} end")).alias(f"bts_usage_{v}_wk_{freq_str}"),
                    F.sum(F.expr(f"case when is_wk = False then {k} end")).alias(f"bts_usage_{v}_wd_{freq_str}"),
                    F.sum(F.expr(f"case when time_slot_grp = 'dt' then {k} end")).alias(f"bts_usage_{v}_dt_{freq_str}"),
                    F.sum(F.expr(f"case when time_slot_grp = 'nt' then {k} end")).alias(f"bts_usage_{v}_nt_{freq_str}"),
                ]
            )

        if service_name == "total":
            _df = df_usage.groupBy("msisdn", "bts_id").agg(*aggs)
        else:
            _df = df_usage.where(f"SERVICENAME IN ({service_name})").groupBy("MSISDN", "bts_id").agg(*aggs)

        dfs.append(_df)

    # merge to 1 df fts
    usage_fts = dfs[0]
    for _df in dfs[1:]:
        usage_fts = usage_fts.join(_df, ["msisdn", "bts_id"], how="outer")
    
    # write to parquet
    out_dir = f"{bts_usage_out_dir}/{freq_str}/date={date_str}"
    fts_utils.save_to_s3(usage_fts, out_dir)

    df_usage_raw.unpersist()


## main
run_mode = '' 
config = utils.load_config("../config/config_feature.yaml")
usage_table_name = 'blueinfo_ocs_crs_usage' 
usage_table_name_on_production = config['config']['table_name'][usage_table_name]

bts_usage_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_usage")
bts_mapping_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_mapping")

spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)
while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    # bts usage daily
    gen_bts_usage_daily_fts(spark, usage_table_name, bts_usage_dir, bts_mapping_dir, run_date)

    start_date = start_date + relativedelta(days=1)