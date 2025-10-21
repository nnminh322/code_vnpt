import sys
from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from src.utils import common as utils 
from src.utils.job_quality_utils import status_check, run_create_feature

@status_check
def pull_usage_mapping_daily(spark, table_name, run_date, out_dir, **kwargs): #
    run_mode = kwargs.get("run_mode", "prod")
    print(f"pulling usage daily for:", run_date)
    query = f"""
        SELECT MSISDN, IMEI
        FROM {table_name} 
        WHERE 1=1
            AND s3_file_date = '{run_date}' 
            AND IMEI <> ''
    """

    df = utils.spark_read_data_from_singlestore(spark, query).persist()
    df.count()
    if run_mode == 'prod': 
        df = df.where("left(msisdn, 4) = '0084' and length(msisdn) = 13")
     
    df = df.withColumn('TAC', F.substring(F.col("IMEI"), 1, 8))
    df = df.drop("IMEI")

    out_dir = f"{out_dir}/daily/date={run_date}"
    utils.save_to_s3(df, out_dir)

if __name__ == "__main__":

    table_name = 'blueinfo_ocs_crs_usage'

    run_create_feature(
        func_and_kwargs=(
            (pull_usage_mapping_daily, {}),
        ),
        global_kwargs={
            'freq': 'daily',
            'table_name': table_name,
            'feature_name': 'tac_mapping',
        }
    )