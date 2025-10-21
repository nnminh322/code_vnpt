import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

dedicated_id = {
    'time': (894, 82, 2678, 275, 871, 283, 2803, 2800, 630, 2663, 282, 111, 
             2816, 540, 294, 295, 288, 640, 252, 285, 655, 2672, 62, 2682, 
             262, 2812, 2834, 272, 2681, 670, 271, 2677, 2832, 570, 8, 292, 
             450, 273, 2801, 920, 650, 1008, 115, 1282, 1262, 1283, 112, 334, 
             2815, 331, 333, 350, 2664, 2829, 2830, 665, 930, 2824, 2802, 1062, 
             2817, 1272, 2691),
    'money': (9001, 7, 2827, 162, 4, 142, 2821, 2701, 9953, 77, 2, 9998, 16, 5, 
              9949, 32, 3, 9999, 31, 1007, 1002, 1162, 1004, 330, 1001, 2700, 322, 66),
    'data': (28, 1212, 965, 124, 1109, 129, 113, 5011, 400, 104, 125, 501, 103, 1409, 
             105, 128, 1209, 2660, 401, 102, 126, 106, 127, 1103, 1102, 3001, 4012, 
             155, 950, 7071, 960, 7082, 5012, 1104, 26),
    'sms': (2661, 88, 263, 284, 2690, 255, 99, 42, 253, 122, 286, 2679, 6, 940, 
            1263, 1006, 2814, 1122, 1284, 2813, 2831, 855, 755, 110, 2825, 2822, 1042, 2692)
}

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

unit_type_str = f"""
    case
        when DEDICATEDACCOUNTID IN {dedicated_id['time']} then 'time'
        when DEDICATEDACCOUNTID IN {dedicated_id['data']} then 'data'
        when DEDICATEDACCOUNTID IN {dedicated_id['money']} then 'money'
        when DEDICATEDACCOUNTID IN {dedicated_id['sms']} then 'sms'
        else 'others'
    end
"""

@status_check
def gen_usageda_daily_fts(spark, run_date, table_name,bt_table_name,bt_msisdn_column, out_dir, run_mode, common_config, **kwargs):
    freq_str = "daily"
    query = f"""
            SELECT MSISDN, ORIGINALTIMESTAMP, 
            DEDICATEDACCOUNTID,UNITS, s3_file_date
            FROM {table_name} 
            WHERE 1=1
                AND s3_file_date = '{run_date}' 
        """
    
    df = utils.spark_read_data_from_singlestore(spark, query).persist()
    df.count() 

    if run_mode == 'prod': 
        df = df.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    df = (
            df
                .withColumnRenamed("MSISDN", "msisdn")
                .withColumn("timestamps", F.to_timestamp("ORIGINALTIMESTAMP", "yyyyMMdd'T'HH:mm:ss"))
                .withColumn("is_wk", F.expr(is_wk_str))
                .withColumn("hour", F.date_format("timestamps", "hh"))
                .withColumn("time_slot_grp", F.expr(time_slot_grp_str))
                .withColumn("unit_type", F.expr(unit_type_str))
        )

    unit_type = ['money', 'time', 'data', 'sms']
    res = []
    for ut in unit_type:
        _df = df.filter(F.col('unit_type') == ut)
        aggs = [F.count("*").alias(f"usageda_{ut}_count_{freq_str}")]
        aggs.extend(
        [
            F.sum(F.expr(f"case when is_wk = True then 1 end")).alias(f"usageda_{ut}_count_wk_{freq_str}"),
            F.sum(F.expr(f"case when is_wk = False then 1 end")).alias(f"usageda_{ut}_count_wd_{freq_str}"),
            F.sum(F.expr(f"case when time_slot_grp = 'dt' then 1 end")).alias(f"usageda_{ut}_count_dt_{freq_str}"),
            F.sum(F.expr(f"case when time_slot_grp = 'nt' then 1 end")).alias(f"usageda_{ut}_count_nt_{freq_str}"),
        ])
        if unit_type != 'sms':
            aggs.extend(
            [
                F.sum('UNITS').alias(f"usageda_{ut}_sum_{freq_str}"),
                F.sum(F.expr(f"case when is_wk = True then UNITS end")).alias(f"usageda_{ut}_sum_wk_{freq_str}"),
                F.sum(F.expr(f"case when is_wk = False then UNITS end")).alias(f"usageda_{ut}_sum_wd_{freq_str}"),
                F.sum(F.expr(f"case when time_slot_grp = 'dt' then UNITS end")).alias(f"usageda_{ut}_sum_dt_{freq_str}"),
                F.sum(F.expr(f"case when time_slot_grp = 'nt' then UNITS end")).alias(f"usageda_{ut}_sum_nt_{freq_str}"),
            ])
        _df_out = _df.groupBy("msisdn").agg(*aggs)
        res.append(_df_out)
    df_out = res[0]
    for r in res[1:]:
        df_out = df_out.join(r, how='outer', on='msisdn')
    out_dir = f"{out_dir}/{freq_str}/date={run_date}"
    utils.save_to_s3(df_out, out_dir)
    df.unpersist()

if __name__ == "__main__":

    table_name = 'blueinfo_ocs_crs_usageda'

    run_create_feature(
        func_and_kwargs=(
            (gen_usageda_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'usageda',

        }
    )
    