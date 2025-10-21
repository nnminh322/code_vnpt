from botocore.config import Config
from pyspark.sql import SparkSession
import subprocess
import boto3
import joblib
import tempfile
import logging
import config
import argparse
import shlex

def create_spark_instance():
    spark = SparkSession.builder\
            .appName('SparkApp') \
            .config("spark.jars", 
            "/staging/libs/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar, \
            /staging/libs/mariadb-java-client-3.1.4.jar, \
            /staging/libs/singlestore-jdbc-client-1.1.5.jar, \
            /staging/libs/commons-dbcp2-2.9.0.jar, \
            /staging/libs/commons-pool2-2.11.1.jar, \
            /staging/libs/spray-json_3-1.3.6.jar") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()
    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set('fs.cos.datalake.endpoint', config.endpoint_url)
    hconf.set('fs.cos.datalake.access.key', config.aws_access_key_id)
    hconf.set('fs.cos.datalake.secret.key', config.aws_secret_access_key)
    return spark

def send_msg_to_telegram(msg):
    url = f"https://api.telegram.org/bot7342706784:AAFkdnM5peDShhYRA2mU-iiC927bDo5JAlE/sendMessage?chat_id=-1002184124681&text={msg}"
    proxy_server = "http://10.144.13.144:3129"
    result = subprocess.run(['curl','-x', proxy_server, url])
    print(result)

def get_s3_client():
    s3 = boto3.client('s3',
                     endpoint_url = 'http://access.icos.datalake.vnpt.vn:80',
                     aws_access_key_id = '47yFLHe5c6Vy9mJaF9fQ',
                     aws_secret_access_key = 'TZBxi4oQLmeo1nXyWkfwIGORM4RIuDSMaZntykuS',
                     config = Config(signature_version = 's3v4'))
    return s3

def save_model_to_s3(model, bucket, key):
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            joblib.dump(model, fp)
            fp.seek(0)
            s3_client.put_object(Body=fp.read(), Bucket = bucket, Key = key)
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)

def load_model_from_s3(bucket, key):
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            s3_client.download_fileobj(Fileobj=fp, Bucket = bucket, Key = key)
            fp.seek(0)
            return joblib.load(fp)
    except Exception as e:
        raise logging.exception(e)

def spark_read_data_from_singlestore(spark, query):
    df = (spark.read
            .format("singlestore")
            .option("ddlEndpoint", "10.144.101.139")
            .option("user", config.user)
            .option("password", config.password)
            .option("database", "blueinfo_dev")
            .option("query", query)
            .load()
        )
    return df

def get_last_successful_date(spark, log_path, table_name):
    try:
        logs_df = spark.read.parquet(log_path)
        last_successful_date = logs_df.filter(logs_df.table_name == table_name) \
                                      .orderBy(logs_df.last_success.desc()) \
                                      .select("last_success") \
                                      .first()
        return last_successful_date.last_success if last_successful_date else None
    except Exception:
        # If log file doesn't exist or there's an error, return None
        return None


def get_all_args():
    parser = argparse.ArgumentParser(description="Dynamic Argument Parser")
    # Parse known arguments
    args, unknown = parser.parse_known_args()
    parsed_args = {}

    # Add unknown arguments to the dictionary
    for arg in unknown:
        if arg.startswith('--'):
            key = arg.lstrip('-')
            parsed_args[key] = True  # Treat flags without values as True
    return parsed_args

def process_args_to_dict(args_string):
    # Use shlex to properly handle quoted strings
    tokens = shlex.split(args_string)
    args_dict = {}

    # Iterate through tokens and build the dictionary
    i = 0
    while i < len(tokens):
        if tokens[i].startswith("--"):
            key = tokens[i][2:]  # Remove the '--' prefix
            # Check if the next token is a value or another key
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
                args_dict[key] = tokens[i + 1]
                i += 2  # Move past the key and value
            else:
                args_dict[key] = None  # Key with no value
                i += 1  # Move past the key
        else:
            i += 1  # Ignore tokens that do not start with '--'
    return args_dict