from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from datetime import datetime
import sys
import utils
import config
import data_quality
import staging

# Intial Spark instance
spark = utils.create_spark_instance()

# Define date
args = utils.process_args_to_dict(sys.argv[1])
current_date = args['dt_to'] if args['dt_to'] is not None else datetime.now().strftime('%Y%m%d')
utils.send_msg_to_telegram(f'Staging in process for {current_date}')
default_start_date = '20231215'

# Create log
data_quality.create_log_quality(spark, config.data_quality_log_path)

# Execute
for table in list(config.table_config.keys())[:2]:
    last_successful_date = utils.get_last_successful_date(spark, config.raw_log_path, table)
    print(f'Last successful date for {table} is {last_successful_date}')
    check_type = data_quality.get_query(spark, config.quality_conf, config.data_layer, table, current_date, last_successful_date)
    if check_type['type'] == 'skip':
        print(f'Table {table} has been skipped.')
        continue
    data_quality.calculate_from_query(spark, config.quality_conf, config.data_layer, table, config.data_quality_log_path, check_type)
    log_df = spark.read.parquet(config.data_quality_log_path)
    if check_type['type'] == 'first_run':
        if config.table_config[table]['freq'] == 'monthly':
            default_start_date = (datetime.strptime(default_start_date, '%Y%m%d') - relativedelta(months=1)).strftime('%Y%m%d')
        staging.get_data(spark, config.table_config, table, default_start_date, current_date, config.staging_log_path, config.data_path)
        utils.send_msg_to_telegram(f'Table {table} staged for the first time.')
    else:
        status = log_df.where(f"s3_file_date = {check_type['_date']} AND status = 'pass'").select('status').limit(1).collect()[0][0]
        if status == 'pass':
            start_date = last_successful_date if last_successful_date else default_start_date
            staging.get_data(spark, config.table_config, table, start_date, current_date, config.staging_log_path, config.data_path)
            print(f'Table {table} staged.')
        else:
            utils.send_msg_to_telegram(f'Table {table} failed for staging.')