import yaml
import os
import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
import shlex
import requests
import logging
logger = logging.getLogger(__name__)
import sys
from datetime import datetime
from src.utils.job_quality_utils import JobQuality
import joblib 
import tempfile
import subprocess


def find_folder(folder_name):
    current_dir = os.path.abspath(__file__)
    while current_dir != os.path.dirname(current_dir):
        possible_path = os.path.join(current_dir, folder_name)

        if os.path.isdir(possible_path):
            return possible_path

        current_dir = os.path.dirname(current_dir)


def load_config(folder_name, config_name):
    config_dir = find_folder(folder_name)
    yaml_path = os.path.join(config_dir, config_name)

    with open(yaml_path, "r") as file: 
        config = yaml.safe_load(file)
    return config 


def load_check_dir(table, freq, config):
    df = None
    if freq == 'daily':
        check_dir = config['sub_dir'][table]
        
    if freq == 'weekly':
        feature_destination = config['feature_destination']
        
        if table == 'usage':
            check_dir = feature_destination['usage_data']['destination'][:-5]
        elif table == 'smrs': 
            check_dir = feature_destination['smrs_3']['destination'][:-5]
        elif table == 'device': 
            check_dir = feature_destination['device']['destination'][:-5]
        else:
            check_dir = feature_destination[table]['destination'].replace("date=", "")
        
    return check_dir


config = load_config("config_edit", "to_edit.yaml")


def create_spark_instance(run_mode = 'prod', python_file = ''):
    clear_checkpoint_dir(run_mode)
    spark = SparkSession.builder \
            .appName(f'node_{python_file}_{run_mode}') \
            .config("spark.jars", "/code/libs/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/code/libs/singlestore-jdbc-client-1.1.5.jar,/code/libs/commons-dbcp2-2.9.0.jar,/code/libs/commons-pool2-2.11.1.jar,/code/libs/spray-json_3-1.3.6.jar") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .getOrCreate()

    sc = spark.sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set("fs.cos.datalake.endpoint", config['s3']['s3_endpoint'])
    hconf.set("fs.cos.datalake.access.key", config['s3']['s3_access_key'])
    hconf.set("fs.cos.datalake.secret.key", config['s3']['s3_secret_key'])
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    if run_mode == 'prod': 
        spark.sparkContext.setCheckpointDir(f"{config['s3']['bucket_name_cos']}/tmp/prod_checkpoints")
    else: 
        spark.sparkContext.setCheckpointDir(f"{config['s3']['bucket_name_cos']}/tmp/bt_checkpoints")
    sc.setLogLevel("WARN")

    return spark


def clear_checkpoint_dir(run_mode):
    
    bucket_name = config['s3']['bucket_name']
    if run_mode == 'prod':
        checkpoint_prefix = "tmp/prod_checkpoints/"
    else: 
        checkpoint_prefix = "tmp/bt_checkpoints/"

    s3 = get_s3_client()

    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=checkpoint_prefix)

    if "Contents" in objects:
        delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
        print(f"Deleted checkpoint directory: {checkpoint_prefix}")
    else:
        print(f"Not found checkpoint!!")


def spark_read_data_from_singlestore(spark, query, table_name_dev=''):
    # dung table name dev de co dinh 
    if table_name_dev in ('blueinfo_dbvnp_prepaid_subscribers_history', 'blueinfo_ccbs_ccs_xxx_danhba_dds_pttb') or 'prepaid' in table_name_dev or 'danhba' in table_name_dev:
        db_name = config['database_name']['db1']
    else: 
        db_name = config['database_name']['db2']

    df = (spark.read
        .format("singlestore")
        .option("ddlEndpoint", "10.144.101.140")
        .option("user", config['single_store']['user'])
        .option("password", config['single_store']['password'])
        .option("database", db_name)
        .option("query", query)
        .load()
        )
    return df


def save_to_s3(df, write_dir, mode="overwrite"):
    df.write.parquet(f"{config['s3']['bucket_name_cos']}/{write_dir}", mode=mode)


def load_from_s3(spark, load_dir, merge_schema = 'false'): # chua can biet co dung merge schema khong nen chua define
    df = spark.read.option('mergeSchema', merge_schema).parquet(f"{config['s3']['bucket_name_cos']}/{load_dir}")
    return df


def process_args_to_dict(args_string):
    # Use shlex to properly handle quoted strings
    tokens = shlex.split(args_string)
    args_dict = {}

    ## storage index of the key 
    lst_tokens = []
    lst_idx = []

    # Iterate through tokens and build the dictionary
    i = 0
    while i < len(tokens):
        if tokens[i].startswith("--"):
            key = tokens[i][2:]  # Remove the '--' prefix
            
            lst_tokens.append(key)
            lst_idx.append(i)
            
            # Check if the next token is a value or another key
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
                args_dict[key] = tokens[i + 1]
                i += 2  # Move past the key and value
            else:
                args_dict[key] = None  # Key with no value
                i += 1  # Move past the key
        else:
            i += 1  # Ignore tokens that do not start with '--'

    # # handle rieng cho truong hop cua skip_dags (vi shlex split ko bat duoc list)
    idx = lst_tokens.index("skip_dags")
    idx_token = lst_idx[idx]
    idx_next_token = lst_idx[idx+1]
    args_dict["skip_dags"] = ''.join([tokens[i] for i in range(idx_token+1, idx_next_token)])
    
    return args_dict


def send_msg_to_telegram(msg, bot_id="7875188738:AAHGkJt-eXpQnt3DvNJlwQeHGq4N_xBQDrc", chat_id = -1002184124681):
    url = f"https://api.telegram.org/bot{bot_id}/sendMessage"
    params = {"chat_id": chat_id, "text": msg}
    proxy_server = {"http": "http://10.144.13.144:3129", "https": "http://10.144.13.144:3129"}
   
    try: 
        response = requests.post(url, params=params, proxies=proxy_server)
        result = response.json()

        if result['ok'] is not True:
            logger.warning(f"Tele msg failed with error: {result.get('description')}, Respone: {result}")

    except Exception as e:
        logger.warning(f"FAILED send msg {e}")


def get_s3_client():
    cos_endpoint = config['s3']['s3_endpoint']
    cos_access_key = config['s3']['s3_access_key']
    cos_secret_key = config['s3']['s3_secret_key']  

    s3 = boto3.client('s3',
                     endpoint_url = cos_endpoint,
                     aws_access_key_id = cos_access_key,
                     aws_secret_access_key = cos_secret_key,
                     config = Config(signature_version = 's3v4'))
    return s3

def save_pickle(model, key):
    bucket = config['s3']['bucket_name']
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            joblib.dump(model, fp)
            fp.seek(0)
            s3_client.put_object(Body=fp.read(), Bucket = bucket, Key = key)
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)

def load_pickle(key):
    bucket = config['s3']['bucket_name']
    s3_client = get_s3_client()
    try:
        with tempfile.TemporaryFile() as fp:
            s3_client.download_fileobj(Fileobj=fp, Bucket = bucket, Key = key)
            fp.seek(0)
            return joblib.load(fp)
    except Exception as e:
        raise logging.exception(e)

def install_package(users=True):

    file_path = ["xgboost==2.1.1", "scipy==1.8.1", "optbinning==0.20.1", "catboost==1.2.7", "pandas==1.4.4"]
    if users:
        sys.path.append("/.local/lib/python3.9/site-packages")
        cmd = [sys.executable, "-m", "pip", "install", "--user", "--no-cache-dir", "--proxy", config['proxy']] + file_path
    else: 
        cmd = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--proxy", config['proxy']] + file_path

    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Successfully install packages")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error when install packages", e)
        print(e.stdout)
        print(e.stderr)