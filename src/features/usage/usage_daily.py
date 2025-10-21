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
def gen_usage_daily_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):

    freq_str = "daily"
    
    print("generating daily fts for:", run_date)

    query = f"""
        SELECT MSISDN, ORIGINALTIMESTAMP, ORIGINATINGCELLID,
            DURATION, VOLUME, TOTALCOST / 1000 as TOTALCOST,
            CHARGINGCATEGORYNAME, SERVICENAME
        FROM {table_name} 
        WHERE 1=1
            AND s3_file_date = '{run_date}' 
    """

    # load data
    df_usage_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
    df_usage_raw.count()
    
    if run_mode == 'prod': 
        df_usage_raw = df_usage_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")

    df_usage = (
        df_usage_raw
            .withColumnRenamed("MSISDN", "msisdn")
            .withColumn("timestamps", F.to_timestamp("ORIGINALTIMESTAMP", "yyyyMMdd'T'HH:mm:ss"))
            .withColumn("is_wk", F.expr(is_wk_str))
            .withColumn("hour", F.date_format("timestamps", "hh"))
            .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
    )
    
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

        aggs = [F.count("*").alias(f"usage{_tmp.lower()}_count_{freq_str}")]

        # create mutilple agg then group by
        for k, v in _dict.items():
            if k != '1':
                aggs.extend([F.sum(F.col(k)).alias(f"usage_{v}_{freq_str}")])
                
            aggs.extend(
                [
                    F.sum(F.expr(f"case when is_wk = True then {k} end")).alias(f"usage_{v}_wk_{freq_str}"),
                    F.sum(F.expr(f"case when is_wk = False then {k} end")).alias(f"usage_{v}_wd_{freq_str}"),
                    F.sum(F.expr(f"case when time_slot_grp = 'dt' then {k} end")).alias(f"usage_{v}_dt_{freq_str}"),
                    F.sum(F.expr(f"case when time_slot_grp = 'nt' then {k} end")).alias(f"usage_{v}_nt_{freq_str}"),
                ]
            )

        if service_name == "total":
            _df = df_usage.groupBy("msisdn").agg(*aggs)
        else:
            _df = df_usage.where(f"SERVICENAME IN ({service_name})").groupBy("msisdn").agg(*aggs)

        # write to parquet
        if _tmp == "":
            _tmp = "_total"
        _tmp = _tmp.replace("_", "")
        usage_out_dir = f"{out_dir + _tmp}/{freq_str}/date={run_date}"
        utils.save_to_s3(_df, usage_out_dir)

    df_usage_raw.unpersist()


if __name__ == "__main__":
    table_name = "blueinfo_ocs_crs_usage"

    run_create_feature(
        func_and_kwargs=(
            (gen_usage_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'usage',

        }
    )
