from pyspark.sql import SparkSession
from datetime import datetime
import urllib.parse
import utils

# Intial Spark instance
spark = utils.create_spark_instance()

# Staging
def resize_df(df, target_partition_size_mb=128):
    # Estimate the size in bytes using Spark's JVM SizeEstimator
    size_bytes = df._sc._jvm.org.apache.spark.util.SizeEstimator.estimate(df._jdf)

    # Convert size to MB
    size_mb = size_bytes / (1024 ** 2)

    # Get the current number of partitions
    current_partitions = df.rdd.getNumPartitions()

    # Calculate the number of partitions
    n_partitions = max(1, int(size_mb / target_partition_size_mb))

    # Determine whether to use coalesce or repartition
    if n_partitions < current_partitions:
        df = df.coalesce(n_partitions)
    elif n_partitions > current_partitions:
        df = df.repartition(n_partitions)
    return df

def sanitize_id_suffix(id_suffix):
    return urllib.parse.quote(id_suffix, safe="")

def log_execution_to_parquet(spark, staging_log_path, table_name, last_date, run_date):
    log_data = [(table_name, last_date, run_date)]
    log_df = spark.createDataFrame(log_data, schema=["table_name", "last_success", "run_date"])
    log_df.write.mode("append").parquet(staging_log_path)

def generate_query(spark, table_config, table_name, start_date, end_date):
    frequency = table_config[table_name]["freq"]
    partition_col = table_config[table_name]["partition_col"]

    # Initial query to find id_suffixes
    _start = start_date if frequency == 'daily' else start_date[:6]
    _end = end_date if frequency == 'daily' else end_date[:6]
    initial_query = f"""
        SELECT LEFT({partition_col}, 1) as id_suffix, s3_file_date
        FROM {table_name}
        WHERE s3_file_date > '{_start}'
        AND s3_file_date <= '{_end}'
        GROUP BY s3_file_date, LEFT({partition_col}, 1)
    """
    if int(_start) < int(_end):
        # Fetch unique id_suffix values
        df = utils.spark_read_data_from_singlestore(spark, initial_query).toPandas()
        if df.empty:
            return []
        grouped_data = df.groupby('s3_file_date')['id_suffix'].apply(list).to_dict()

        # Prepare the result dictionary
        queries = []
        for _date, id_suffixes in grouped_data.items():
            for _id in id_suffixes:
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE s3_file_date = '{_date}'
                    AND LEFT({partition_col}, 1) = '{_id}'
                """
                queries.append({
                    "date": _date,
                    "id_suffix": _id,
                    "id_suffix_std": sanitize_id_suffix(_id),
                    "query": query.strip(),
                })
        return queries
    else:
        return []

def get_data(spark, table_config, table_name, start_date, end_date, staging_log_path, data_path):
    queries = generate_query(spark, table_config, table_name, start_date, end_date)
    if not queries:
        print(f"Skipping table {table_name} as no queries were generated.")
        return
    table_success = True  # Track success for the entire table
    n_partitions = spark.sparkContext.defaultParallelism * 3
    for item in queries:
        _query = item['query']
        _date = item['date']
        _id = item['id_suffix_std']
        try:
            df = utils.spark_read_data_from_singlestore(spark, _query)
            df = df.repartition(n_partitions).persist()
            out_path = data_path + f"table_name={table_name}/s3_file_date={_date}/partition_col={_id}"
            if df.count() > 0:
                df = resize_df(df)
                df.write.mode('overwrite').parquet(out_path)
    
            # clear memory
            df.unpersist(True)
        except Exception as e:
            table_success = False
            print(f"Error processing table: {table_name}, date: {_date}, id_suffix: {_id}. Query: {_query}. Error: {e}")
    if table_success:
        log_execution_to_parquet(spark, staging_log_path, table_name, queries[-1]['date'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print("Success table", table_name)
    else:
        print(f"Skipping logging for table {table_name} due to incomplete processing.")