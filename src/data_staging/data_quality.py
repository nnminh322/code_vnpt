from pyspark.sql import functions as F, types as T
from datetime import datetime
import utils

# Check for data quality
def auto_cast_df(spark, df, data_quality_log_path):
    origin_schema = spark.read.parquet(data_quality_log_path).schema
    # Create a mapping of column names to their origin data types
    origin_type_mapping = {field.name: field.dataType for field in origin_schema.fields}

    # Create a list of cast expressions for columns
    cast_expressions = []
    for field in df.schema.fields:
        field_name = field.name

        # Check if the field exists in the origin schema
        if field_name in origin_type_mapping:
            origin_field_type = origin_type_mapping[field_name]

            # Cast the column to the type defined in the origin schema
            cast_expressions.append(df[field_name].cast(origin_field_type).alias(field_name))
    casted_df = df.select(*cast_expressions)
    return casted_df

def create_log_quality(spark, data_quality_log_path):
    try:
        # Try to read the Parquet file
        log_df = spark.read.parquet(data_quality_log_path)
        # Perform a simple operation to confirm it's accessible
        log_df.count()
        print(f"Log file already exists at: {data_quality_log_path}")
    except Exception as e:
        # Handle missing or corrupted file by creating a new one
        print(f"Log file not found or unreadable at {data_quality_log_path}. Creating a new log file.")
        schema = T.StructType([
            T.StructField("data_layer", T.StringType(), True),    # String, nullable
            T.StructField("table_name", T.StringType(), True),    # String, nullable
            T.StructField("metric", T.StringType(), True),        # String, nullable
            T.StructField("metric_value", T.DoubleType(), True),  # Double, nullable
            T.StructField("value_compare", T.DoubleType(), True), # Double, nullable
            T.StructField("threshold", T.DoubleType(), True),     # Double, nullable
            T.StructField("status", T.StringType(), True),        # String, nullable
            T.StructField("s3_file_date", T.StringType(), True),  # String, nullable
            T.StructField("run_date", T.StringType(), True)         # Date, nullable
        ])

        # Create an empty DataFrame with the schema
        empty_df = spark.createDataFrame([], schema)
        
        # Write the empty DataFrame to the specified path
        empty_df.write.parquet(data_quality_log_path)
        print(f"New log file created at: {data_quality_log_path}")

def calculate_moving_average(log_df, data_layer, table_name, metric, query_date, n=3):
    # Filter relevant rows
    filtered_df = log_df.where(
        f"(data_layer='{data_layer}') AND (table_name='{table_name}') AND (metric='{metric}') AND s3_file_date < '{query_date}'"
    ).withColumn('run_datetime', F.substring('run_date', 0, 10)).drop_duplicates(["s3_file_date", "run_datetime"]).orderBy(F.col("s3_file_date").desc())
    
    # Collect the last `n` metric values
    values = filtered_df.select("metric_value").limit(n).collect()
    if len(values) < n:
        return None  # Not enough data for moving average
    
    # Calculate the moving average
    return sum([row["metric_value"] for row in values]) / n

def get_query(spark, quality_conf, data_layer, table_name, query_date, last_successful_date):
    frequency = quality_conf["row_count"][data_layer][table_name]["freq"]
    check_type = {
        '_date': query_date if frequency == 'daily' else query_date[:6]
    }
    # check_type = config.check_type
    if last_successful_date is None:
        check_type['query'] = f"""
            SELECT
                s3_file_date, COUNT(1) AS row_count
            FROM 
                {table_name}
            WHERE s3_file_date <= '{check_type["_date"]}'
            GROUP BY s3_file_date
        """
        check_type['type'] = 'first_run'
    elif last_successful_date < check_type["_date"]:
        check_type['query'] = f"""
            SELECT
                s3_file_date, COUNT(1) AS row_count
            FROM 
                {table_name}
            WHERE s3_file_date = '{check_type["_date"]}'
            GROUP BY s3_file_date
        """
        check_type['type'] = 'check'
    elif last_successful_date == check_type["_date"]:
        check_type['type'] = 'skip'
    return check_type

def calculate_from_query(spark, quality_conf, data_layer, table_name, data_quality_log_path, check_type):
    threshold = quality_conf["row_count"][data_layer][table_name]["threshold"]
    # Run the query and get data quality results
    df = utils.spark_read_data_from_singlestore(spark, check_type['query'])
    df = df.withColumn("data_layer", F.lit(data_layer)) \
        .withColumn("table_name", F.lit(table_name)) \
        .withColumn("metric", F.lit("row_count")) \
        .withColumn("metric_value", F.col("row_count")) \
        .withColumn("run_date", F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    if check_type['type'] == 'first_run':
        df = df.withColumn("value_compare", F.lit(None)) \
            .withColumn("threshold", F.lit(None)) \
            .withColumn("status", F.lit(None))
    else:
        log_df = spark.read.parquet(data_quality_log_path)
        ma3 = calculate_moving_average(log_df, data_layer, table_name, "row_count", check_type['_date'], n=3)
        df = df.withColumn("value_compare", F.lit(ma3)) \
            .withColumn(
                "status",
                F.when(F.abs(F.col("metric_value") - F.col("value_compare")) / F.col("value_compare") <= threshold, "pass")
                    .otherwise("fail")
            ) \
            .withColumn("threshold", F.lit(threshold))
    # Write the updated log to the path
    df = auto_cast_df(spark, df, data_quality_log_path)
    df.drop("row_count").write.mode("append").parquet(data_quality_log_path)
    print("Log updated successfully.")