import re
import json
import io
import gzip
import zipfile
import requests
import pandas as pd 
from src.utils import common as utils
from datetime import datetime 
from pyspark.sql import functions as F, types as T, SparkSession, Row, DataFrame as SparkDataFrame
from pyspark.sql.window import Window

config = utils.load_config("config_edit", "to_edit.yaml")

def calculate_status(row, moving_average_dict=None):
    """
    Calculate the data quality status for a row based on thresholds and moving average.
    Args:
        row (pd.Series): Row with metric_value, thresholds, and layer info.
        moving_average_dict (dict): Optional dict of feature_name to moving average.
    Returns:
        str: 'PASS', 'WARNING', or 'ERROR'.
    """
    hard_threshold = row['hard_threshold']
    soft_threshold = row['soft_threshold']
    layer = row['layer']

    status = 'PASS'
    if moving_average_dict is not None: 
        ma = moving_average_dict.get(row['feature_name'])
        if ma is None: # not enough observations
            status = 'PASS'
        else: 
            compare_value = abs(row['metric_value'] - ma)/ma
            if compare_value > hard_threshold: 
                status = 'ERROR' if layer == 'database' else 'WARNING'
            elif soft_threshold <= compare_value <= hard_threshold: 
                status = 'WARNING'
            else:
                status = 'PASS'
    return status

def create_log_quality(spark, data_quality_log_path):
    """
    Ensure the data quality log file exists at the given path, or create a new one with the correct schema.
    Args:
        spark (SparkSession): The Spark session.
        data_quality_log_path (str): Path to the log file (S3 or local).
    """
    try: 
        log_df = utils.load_from_s3(spark, data_quality_log_path)
        # Perform a simple operation to confirm it's accessible
        log_df.count()
        print(f"Log file already exists at: {data_quality_log_path}")
    except: 
#         Handle missing or corrupted file by creating a new one
        print(f"Log file not found or unreadable at {data_quality_log_path}. Creating a new log file.")
        schema = T.StructType([
            T.StructField("feature_name", T.StringType(), True),    # String, nullable
            T.StructField("metric_value", T.StringType(), True),  # Double, nullable
            T.StructField("log_date", T.StringType(), True),  # String, nullable
            T.StructField("layer", T.StringType(), True),    # String, nullable
            T.StructField("snapshot", T.StringType(), True),    # String, nullable
            T.StructField("destination", T.StringType(), True),    # String, nullable
            T.StructField("dimension", T.StringType(), True),    # String, nullable
            T.StructField("metric_name", T.StringType(), True),    # String, nullable
            T.StructField("severity", T.StringType(), True),    # String, nullable
            T.StructField("hard_threshold", T.DoubleType(), True),  # Double, nullable
            T.StructField("soft_threshold", T.DoubleType(), True),  # Double, nullable
            T.StructField("method", T.StringType(), True),  # Double, nullable
            T.StructField("status", T.StringType(), True),  # Double, nullable
            
        ])
        # Create an empty DataFrame with the schema
        empty_df = spark.createDataFrame([], schema)
        
        # Write the empty DataFrame to the specified path
        utils.save_to_s3(empty_df, data_quality_log_path)
        print(f"New log file created at: {data_quality_log_path}")

def update_log_data(spark, df_result, output_path):
    """
    Append new data quality results to the log file and save.
    Args:
        spark (SparkSession): The Spark session.
        df_result (Spark DataFrame): New DQ results.
        output_path (str): Path to the log file.
    """
    create_log_quality(spark, output_path)
    
    # load log
    existing_log_df = utils.load_from_s3(spark, output_path).cache()
    existing_log_df.count() 

    # union data 
    combined_df = df_result.unionByName(existing_log_df, allowMissingColumns=True)

    utils.save_to_s3(combined_df, output_path, mode="append")    
    spark.catalog.clearCache()

def execute_query(spark, query, table, source='database'):
    """
    Execute a SQL query on the specified source (Spark or database).
    Args:
        spark (SparkSession): The Spark session.
        query (str): SQL query string.
        source (str): 'database' or other (e.g., 's3').
    Returns:
        Spark DataFrame: Query result.
    """
    if source != 'database': 
        query = re.sub(r"s3_file_date\s*=\s*'[^']+'", "1=1", query.lower())
        result = spark.sql(query)
    else: 
        result = utils.spark_read_data_from_singlestore(spark, query, table) 
    
    return result

def get_schemas(spark, source, table):
    """
    Get the schema (column names and types) for a table from the source.
    Args:
        spark (SparkSession): The Spark session.
        source (str): 'database' or other (e.g., 's3').
        table (str): Table name.
    Returns:
        pd.DataFrame: DataFrame with columns 'col_name' and 'data_type'.
    """
    if source == 'database':
        query = f"""
            SELECT 
                column_name AS col_name, 
                data_type AS data_type
            FROM information_schema.columns 
            WHERE table_name = '{table}'
        """
        schema_df = execute_query(spark, query, table, source)
    else:
        describe_df = execute_query(spark, f"DESCRIBE {table}", table, source)
        for col in describe_df.columns:
            describe_df = describe_df.withColumnRenamed(col, col.lower())
        schema_df = describe_df.select("col_name", "data_type")

    return schema_df.toPandas()


def get_past_logs(spark, log_path, snapshot, dimension, metric_name, severity, destination):
    """
    Retrieve past data quality logs filtered by snapshot, dimension, metric, severity, and destination.
    Args:
        spark (SparkSession): The Spark session.
        log_path (str): Path to the log file.
        snapshot (str): Snapshot date string.
        dimension (str): DQ dimension.
        metric_name (str): Metric name.
        severity (str): Severity level.
        destination (str): Destination name.
    Returns:
        Spark DataFrame: Filtered log DataFrame.
    """
    try: 
        log_df = utils.load_from_s3(spark, log_path)
        filter_df = log_df.filter(F.expr(f"""
                        snapshot < '{snapshot}' and 
                        dimension = '{dimension}' and 
                        metric_name = '{metric_name}' and 
                        severity = '{severity}' and
                        destination = '{destination}'
                        """))
        return filter_df
    except:
        schema = T.StructType([
            T.StructField("feature_name", T.StringType(), True),    # String, nullable
            T.StructField("metric_value", T.DoubleType(), True),  # Double, nullable
            T.StructField("log_date", T.StringType(), True),  # String, nullable
            T.StructField("layer", T.StringType(), True),    # String, nullable
            T.StructField("snapshot", T.StringType(), True),    # String, nullable
            T.StructField("destination", T.StringType(), True),    # String, nullable
            T.StructField("dimension", T.StringType(), True),    # String, nullable
            T.StructField("metric_name", T.StringType(), True),    # String, nullable
            T.StructField("severity", T.StringType(), True),    # String, nullable
            T.StructField("hard_threshold", T.DoubleType(), True),  # Double, nullable
            T.StructField("soft_threshold", T.DoubleType(), True),  # Double, nullable
            T.StructField("method", T.StringType(), True),  # Double, nullable
            T.StructField("status", T.StringType(), True),  # Double, nullable
            
        ])
        return spark.createDataFrame([], schema)

def calculate_moving_average(spark, common_config, snapshot, dimension, metric_name, severity, destination, n_obs=3):
    """
    Calculate the moving average for a metric over the last n_obs snapshots.
    Args:
        spark (SparkSession): The Spark session.
        common_config (dict): Common config with log path info.
        snapshot (str): Current snapshot date.
        dimension (str): DQ dimension.
        metric_name (str): Metric name.
        severity (str): Severity level.
        destination (str): Destination name.
        n_obs (int): Number of observations for moving average.
    Returns:
        dict: feature_name to moving average value (or None if not enough data).
    """
    try:
        # load and prepare data 
        log_path = common_config['outdir']['logs'] + 'log_data_quality'
        filter_df = get_past_logs(spark, log_path, snapshot, dimension, metric_name, severity, destination)
        filter_df = filter_df.withColumn("metric_value", F.col("metric_value").cast(T.DoubleType()))
        
        # extract the date part from log_date and filter to get the most recent record for each date
        filter_df = filter_df.withColumn("date", F.to_date(F.col("log_date").cast("string").substr(1, 8), "yyyyMMdd"))
        window_spec_1 = Window.partitionBy("feature_name", "date").orderBy(F.col("log_date").desc())
        filter_df = filter_df.withColumn("rn", F.row_number().over(window_spec_1))
        filter_df = filter_df.filter(F.col("rn") == 1).drop("rn")

        # filter to get the most recent n_obs records for each feature_name
        window_spec_2 = Window.partitionBy("feature_name").orderBy(F.col("date").desc())
        filter_df = filter_df.withColumn("rn", F.row_number().over(window_spec_2))
        filter_df = filter_df.filter(F.col("rn") <= n_obs)

        # count number of observations per feature_name
        count_df = filter_df.groupBy("feature_name").agg(
            F.count("metric_value").alias("obs_count"),
            F.avg("metric_value").alias("ma")
        )

        # only keep those with enough observations
        count_df = count_df.withColumn(
            "ma_final",
            F.when(F.col("obs_count") == n_obs, F.col("ma")).otherwise(F.lit(None))
        )

        avg_threshold_dict = {row['feature_name'] : row['ma_final'] for row in count_df.collect()}
    except:
        avg_threshold_dict = None

    return avg_threshold_dict

def trace_event(data, layer, bot_id = "8191564358:AAEzYbnBUr9Z_NG7uDWa7sO6vvvgWuZvV9U", chat_id = -1002827082022): 

    # Convert to pandas DataFrame
    if isinstance(data, SparkDataFrame): 
        data = data.toPandas()

    # Filter necessary data for monitor 
    if layer == 'feature': 
        dct_top_features = utils.load_config("configs", "common.yaml")['top_features']
        features = []
        for feature in dct_top_features.values():
            features.extend(feature)
        filtered_df = data[~((data['dimension'] == 'completeness') & (~data['feature_name'].isin(features)))]  
    else: 
        filtered_df = data.copy()
    # convert to list of dicts
    filtered_df = filtered_df.to_dict(orient='records') 

    def tele_send(bot_token, chat_id, proxy_server, file_dir):
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
        with open(file_dir, 'rb') as file: 
            response = requests.post(
                url,
                data={'chat_id':chat_id},
                files={'document': file},
                proxies=proxy_server
            )

    # Insert "table_store:test" as the first item in the list and write data to JSON file
    filtered_df = [f"table_store:log_dq_{layer}"] + filtered_df

    fname = f"dq_{layer}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    with open(fname + '.txt', "w", encoding='utf-8') as f: 
        f.write(json.dumps(filtered_df, ensure_ascii=False, indent=4))
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