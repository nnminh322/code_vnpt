import sys
from src.utils import common as utils, data_quality_utils as dq_utils 
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def execute(spark, snapshot, common_config, layer):
    df = utils.load_from_s3(spark, common_config['outdir']['logs'] + 'log_data_quality')
    df = df.where(f"snapshot = '{snapshot}' and layer = '{layer}'")
    df = df.withColumn("date", F.to_date(F.col("log_date").cast("string").substr(1, 8), "yyyyMMdd"))
    window_spec_1 = Window.partitionBy("feature_name", "date").orderBy(F.col("log_date").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec_1))
    df = df.filter(F.col("rn") == 1).drop("rn")
    
    # check error 
    cnt = df.where("status = 'ERROR'").count() 

    if cnt > 0: 
        utils.send_msg_to_telegram("Found error status in quality!!!", bot_id = "8191564358:AAEzYbnBUr9Z_NG7uDWa7sO6vvvgWuZvV9U", chat_id = -1002827082022)
        raise Exception("DQ error!!!")

if __name__ == '__main__':
    layer = 'database'
    
    spark = utils.create_spark_instance()
    if layer == 'database': 
        rules = utils.load_config("configs", "dq_config_raw.yaml")
    else: 
        rules = utils.load_config("configs", "dq_config_feature.yaml")

    # Parse command-line arguments for date range
    args = utils.process_args_to_dict(sys.argv[1])
    dt_to = datetime.strptime(args['dt_to'], "%Y%m%d") + relativedelta(days=1)
    dt_from = datetime.strptime(args['dt_from'], "%Y%m%d")

    if dt_from.weekday() == 0: 
        run_date = dt_to.strftime('%Y%m%d') # assumption: now
        common_config = utils.load_config("configs", "common.yaml")

        execute(spark, run_date, common_config, layer)

    