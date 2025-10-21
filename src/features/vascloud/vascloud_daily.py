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

vas_service_category_expr = '''
    CASE 
        WHEN service_code regexp 'BAOHIEM|CARE|BACSY|BACSI' THEN 'Healthcare'
        WHEN service_code regexp 'BONGDA|ISPORT|BDAVINASPORT|GAME|ESPORT' THEN 'Sport'
        WHEN service_code regexp 'ENGLISH|STUDY|HOCVUI' THEN 'Study'
        WHEN service_code regexp 'DULICH|TRAVEL|BALODI|TRIP' THEN 'Travel'
        WHEN service_code regexp 'GIAITRI|HAI|PHIM|MUSIC|CLIP|TIKTOK|FACEBOOK|ZINGMP3|SHOPEE|SHOPPING' THEN 'Lifestyle'
        ELSE 'other'
    END
'''
lst_category = re.findall(pattern, vas_service_category_expr)

@status_check
def gen_vascloud_daily_fts(spark, table_name, bt_table_name, bt_msisdn_column, out_dir, run_date, run_mode, common_config):

    ## define query va field sau

    query = f"""SELECT *
                FROM {table_name}
                WHERE 1=1
                    AND s3_file_date = {run_date}
            """ 

    df_raw = utils.spark_read_data_from_singlestore(spark, query).persist()
    df_raw.count()

    if run_mode == 'prod': 
        df_raw = df_raw.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
        
    df = (
        df_raw
            .withColumn("service_category", F.expr(vas_service_category_expr))
            .withColumn("transaction_time", F.to_timestamp(F.col("transaction_time").cast("string"),  "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"))
    )

    aggregations = [ 
        # F.collect_set("service_code").alias(f"vascloud_service_collect_set_daily"),
        F.sum("value").alias(f"vascloud_spend_sum_daily"),
        F.min("value").alias(f"vascloud_spend_min_daily"),
        F.max("value").alias(f"vascloud_spend_max_daily"),
        F.count("value").alias(f"vascloud_spend_count_daily"), 
    ]
    
    for category in lst_category:
        aggregations.extend([
            # F.collect_set(F.when(F.col("service_category") == category, F.col("service_code"))).alias(f"vascloud_{category.lower()}_service_collect_set_daily"),
            F.sum(F.when(F.col("service_category") == category, F.col("value"))).alias(f"vascloud_{category.lower()}_spend_sum_daily"),
            F.min(F.when(F.col("service_category") == category, F.col("value"))).alias(f"vascloud_{category.lower()}_spend_min_daily"),
            F.max(F.when(F.col("service_category") == category, F.col("value"))).alias(f"vascloud_{category.lower()}_spend_max_daily"),
            F.count(F.when(F.col("service_category") == category, F.col("value"))).alias(f"vascloud_{category.lower()}_spend_count_daily"),       
        ])
    
    df_fts = df.groupBy("msisdn").agg(*aggregations)
    
    final_dir = out_dir + f'daily/date={run_date}'
    utils.save_to_s3(df_fts, final_dir)
    df_raw.unpersist()

if __name__ == "__main__":

    table_name = 'blueinfo_vascloud_da'

    run_create_feature(
        func_and_kwargs=(
            (gen_vascloud_daily_fts, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'vascloud',
        }
    )