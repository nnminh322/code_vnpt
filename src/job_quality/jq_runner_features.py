import sys
import zipfile
import json
import requests
from src.utils import common as utils
from pyspark.sql import functions as F, types as T, Window, DataFrame as SparkDataFrame

def tele_send(bot_token, chat_id, proxy_server, file_dir):
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    
    with open(file_dir, 'rb') as file: 
        response = requests.post(
            url,
            data={'chat_id': chat_id},
            files={'document': file},
            proxies=proxy_server
        )
    
    return response

if __name__ == "__main__":
    # Load configuration files
    common_config = utils.load_config("configs", "common.yaml")
    config = utils.load_config("config_edit", "to_edit.yaml")
    
    # Parse command line arguments
    args = utils.process_args_to_dict(sys.argv[1])
    start_time = args['start_time']

    # Initialize Spark session for data processing
    spark = utils.create_spark_instance(python_file="job_quality")
    
    # Load execution logs for the specified time period
    log_path = common_config['job_quality_log'] + f"start_time={start_time}/"
    data = utils.load_from_s3(spark, log_path)
    
    # Telegram bot configuration
    bot_id = "8191564358:AAEzYbnBUr9Z_NG7uDWa7sO6vvvgWuZvV9U"
    chat_id = -1002827082022

    # Format timestamp for readability
    data = data.withColumn('update_time', F.date_format("update_time", "yyyyMMddHHmmss"))

    # Convert Spark DataFrame to Pandas for JSON serialization
    if isinstance(data, SparkDataFrame): 
        data = data.toPandas()

    # Prepare data for JSON export
    df = data.to_dict(orient='records')
    df = ["table_store:log_jq"] + df  # Add metadata header

    # Generate report files
    fname = f"jq_{start_time}"
    
    # Create JSON report file
    with open(fname + '.txt', "w", encoding='utf-8') as f: 
        f.write(json.dumps(df, ensure_ascii=False, indent=4))
    with zipfile.ZipFile(fname + '.zip', "w", compression=zipfile.ZIP_DEFLATED) as zipf: 
        zipf.write(fname + '.txt')

    tele_send(
        bot_token = bot_id,
        chat_id = chat_id,
        proxy_server = {
            "http" : config['proxy'],
            "https" : config['proxy']
        },
        file_dir = fname + '.zip'
    )