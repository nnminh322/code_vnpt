from pyspark.sql import functions as F, types as T
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from .utils import feature_store_utils as fts_utils 
from src.utils import common as utils 

def get_bts_id(spark, bts_table_name, bts_dir, date_str):
    print("get bts data for", date_str)
    
    # get dates
    date_to = datetime.strptime(date_str, "%Y%m%d").replace(day=1)
    last_day_of_month = (date_to - relativedelta(months=1) + relativedelta(day=31)).day
    first_day_str = date_str[:-2] + "01"

    date_from_str = (date_to - relativedelta(days=10)).strftime("%Y%m%d")

    date_to_str = date_to.strftime("%Y%m%d")
    
    out_dir = bts_dir + f"/date={first_day_str}"

    try:
       fts_utils.load_from_s3(spark,out_dir)
    except:
        query = f"""
            SELECT LAC as lac, CI as cell_id, BTS_NAME as bts_id, s3_file_date
            FROM {bts_table_name}
            WHERE s3_file_date >= {date_from_str} and s3_file_date < {date_to_str} 
        """

        df_raw =fts_utils.spark_read_data_from_singlestore(spark, query).persist()
        
        # if count == 0 then skip
        if df_raw.count() == 0:
            return
        
        window_spec = Window.partitionBy("bts_id", "lac", "cell_id")

        df = df_raw.withColumn("max_date", F.max("s3_file_date").over(window_spec))\
                   .where("s3_file_date = max_date").drop("max_date")

        fts_utils.save_to_s3(df, out_dir)
        df_raw.unpersist()

##### main #####
run_mode = '' 
bts_table_name = 'blueinfo_rims_celllac'
config = utils.load_config("../config/config_feature.yaml")
bts_mapping_dir = config['config']['run_mode'][run_mode]['out_dir'].replace("source_name", "bts/bts_mapping")

spark = fts_utils.create_spark_instance(run_mode = run_mode)

start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 12, 1)
start_date += relativedelta(weekday=MO)

while start_date <= end_date:
    run_date = start_date.strftime('%Y%m%d')
    
    # get bts mapping
    get_bts_id(spark, bts_table_name, bts_mapping_dir, run_date)

    start_date = start_date + relativedelta(days=1)