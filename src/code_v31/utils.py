import pandas as pd
import numpy as np

from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession

import time
from datetime import datetime, timedelta

import boto3
from botocore.config import Config

import sys
from config_edit import config
import joblib 
import tempfile 
import logging
import requests
import subprocess


def save_to_s3(df, table_name, parquet_name, run_mode = 'prod'):
    dir_name = config.directory_name[run_mode]
    df.write.parquet(f"{config.bucket_name_cos}/{dir_name}/{table_name}/{parquet_name}", mode='overwrite')

def load_from_s3(spark, table_name, parquet_name, run_mode = 'prod', merge_schema = 'false'):
    dir_name = config.directory_name[run_mode]
    df = spark.read.option('mergeSchema', merge_schema).parquet(f"{config.bucket_name_cos}/{dir_name}/{table_name}/{parquet_name}")
    return df

def save_to_s3_by_date(df, table_name, parquet_name, fix_date_of_month, run_mode = 'prod'):
    dir_name = config.directory_name[run_mode]
    df.write.parquet(f"{config.bucket_name_cos}/{dir_name}/{table_name}/by_date/fix_date_of_month={fix_date_of_month}/date={parquet_name}", mode='overwrite')

def load_from_s3_by_date(spark, table_name, fix_date_of_month, run_mode = 'prod'):
    dir_name = config.directory_name[run_mode]
    df = spark.read.parquet(f"{config.bucket_name_cos}/{dir_name}/{table_name}/by_date/fix_date_of_month={fix_date_of_month}")
    return df

def save_dataset(df, parquet_name):
    df.write.parquet(f"{config.bucket_name_cos}/datasets_v2/{parquet_name}", mode='overwrite')

def load_dataset(spark, parquet_name):
    df = spark.read.parquet(f"{config.bucket_name_cos}/datasets_v2/{parquet_name}")
    return df

def get_s3_client():
    s3 = boto3.client('s3',
                     endpoint_url = config.s3_endpoint,
                     aws_access_key_id = config.s3_access_key,
                     aws_secret_access_key = config.s3_secret_key,
                     config = Config(signature_version = 's3v4'))
    return s3


def save_pickle_to_s3(model, key):
    bucket = config.bucket_name
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            joblib.dump(model, fp)
            fp.seek(0)
            s3_client.put_object(Body=fp.read(), Bucket = bucket, Key = key)
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)

def load_pickle_file(key):
    bucket = config.bucket_name
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            s3_client.download_fileobj(Fileobj=fp, Bucket = bucket, Key = key)
            fp.seek(0)
            return joblib.load(fp)
    except Exception as e:
        raise logging.exception(e)


def load_model_config(spark, path):
    
    df = spark.read.parquet(f"{config.bucket_name_cos}/models/{path}")
    
    return df



def save_scoring_result(df_result, parquet_name):
    
    df_result.write.parquet(f"{config.bucket_name_cos}/results/{parquet_name}.parquet", mode='overwrite')
    


#### create spark instance and connect to s3 

def create_spark_instance(run_mode = 'prod'):
    clear_checkpoint_dir(run_mode)
    spark = SparkSession.builder \
            .appName(f'SparkApp_{run_mode}') \
            .config("spark.jars", "/code/libs/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/code/libs/singlestore-jdbc-client-1.1.5.jar,/code/libs/commons-dbcp2-2.9.0.jar,/code/libs/commons-pool2-2.11.1.jar,/code/libs/spray-json_3-1.3.6.jar") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()

    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set("fs.cos.datalake.endpoint", config.s3_endpoint)
    hconf.set("fs.cos.datalake.access.key", config.s3_access_key)
    hconf.set("fs.cos.datalake.secret.key", config.s3_secret_key)
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    if run_mode == 'prod': 
        spark.sparkContext.setCheckpointDir(f"{config.bucket_name_cos}/tmp/prod_checkpoints")
    else: 
        spark.sparkContext.setCheckpointDir(f"{config.bucket_name_cos}/tmp/bt_checkpoints")
    sc.setLogLevel("WARN")

    return spark

def clear_checkpoint_dir(run_mode):
    
    cos_endpoint = config.s3_endpoint
    cos_access_key = config.s3_access_key
    cos_secret_key = config.s3_secret_key  
    bucket_name = config.bucket_name
    if run_mode == 'prod':
        checkpoint_prefix = "tmp/prod_checkpoints/"
    else: 
        checkpoint_prefix = "tmp/bt_checkpoints/"

    s3 = boto3.client(
        "s3",
        endpoint_url=cos_endpoint,
        aws_access_key_id=cos_access_key,
        aws_secret_access_key=cos_secret_key
    )
    
    while True: 
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=checkpoint_prefix)

        if "Contents" in objects:
            delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
        else:
            print(f"Deleted checkpoint directory: {checkpoint_prefix}")
            break

def spark_read_data_from_singlestore(spark, query, table_name_dev=''):
    # dung table name dev de co dinh 
    if table_name_dev in ('blueinfo_dbvnp_prepaid_subscribers_history', 'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb'):
        db_name = config.db1
    else: 
        db_name = config.db2

    df = (spark.read
        .format("singlestore")
        .option("ddlEndpoint", "10.144.101.140")
        .option("user", config.user)
        .option("password", config.password)
        .option("database", db_name)
        .option("query", query)
        .option("parallelRead.tableCreationTimeoutMS", "60000") 
        .option("parallelRead.materializedTableCreationTimeoutMS", "60000") 

        .load()
        )
    return df

# accepted_back_date = 3
# def check_table_finished(table_name, version, accepted_back_date):

#     s3_file_names = get_s3_file_list(table_name, version)

#     target_file_names = []

#     for i in range(accepted_back_date + 1):

#         target_file_name = f"Merged_{(datetime.now() + timedelta(hours=7) - timedelta(days=i)).strftime('%Y%m%d')}"

#         target_file_names.append(target_file_name)


#     is_finished = False

#     for target_file_name in target_file_names:

#         if (f"features/{table_name+version}/{target_file_name}.parquet" in s3_file_names) and (f"features/{table_name+version}/{target_file_name}.parquet/_SUCCESS" in s3_file_names):
            
            
#             #### check target file name on this month            
#             target_date_saved = datetime.strptime(target_file_name[-8:], '%Y%m%d')
            
#             firt_date_of_month = datetime.today().replace(day=2)
            
#             if target_date_saved >= firt_date_of_month:
                
#                 is_finished = True

#                 break
    

#     return is_finished

def check_table_finished(spark, table_name, ALL_MONTHS, fix_date, run_mode):

    fix_date_of_month = ALL_MONTHS[0] + fix_date
    
    try:
        
        target_file_name = f"merged/date={fix_date_of_month}" 

        load_from_s3(spark,table_name, target_file_name, run_mode)
        
        is_finished = True
        
    except: 
        
        is_finished = False
        
    return is_finished

def get_s3_file_list(table_name, version, run_mode = 'prod'):
    s3 = boto3.client('s3',
                     endpoint_url = config.s3_endpoint,
                     aws_access_key_id = config.s3_access_key,
                     aws_secret_access_key = config.s3_secret_key,
                     config = Config(signature_version = 's3v4'))
    dir_name = config.directory_name[run_mode]
    s3_folder = s3.list_objects_v2(Bucket=config.bucket_name, Prefix=f"{dir_name}/{table_name+version}")
    
    if 'Contents' not in s3_folder.keys():
        
        return []
    
    s3_files = s3.list_objects_v2(Bucket=config.bucket_name, Prefix=f"{dir_name}/{table_name+version}", MaxKeys=99999999)['Contents']

    s3_files_names = list(map(lambda x: x['Key'], s3_files))
    
    return s3_files_names

def send_msg_to_telegram(msg):
    bot_id = "7795284458:AAGPJp_MRX9OGQu5adJWJNESEh2q0c8r0Cg"
    url = f"https://api.telegram.org/bot{bot_id}/sendMessage"
    params = {"chat_id": -1002184124681, "text": msg}
    proxy_server = {"http": f"{config.proxy}", "https": f"{config.proxy}"}
    
    response = requests.post(url, params=params, proxies=proxy_server)
    print(response.json())

def install_package(users=True):

    file_path = ["xgboost==2.1.1", "scipy==1.8.1", "optbinning==0.20.1", "catboost==1.2.7", "pandas==1.4.4"]
    if users:
        sys.path.append("/.local/lib/python3.9/site-packages")
        cmd = [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "--proxy", config.proxy] + file_path
    else: 
        cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--proxy", config.proxy] + file_path

    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Successfully install packages")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error when install packages", e)
        print(e.stdout)
        print(e.stderr)