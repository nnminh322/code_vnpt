"""
Job Quality Monitoring System

A comprehensive monitoring and execution framework for Apache Spark-based ETL pipelines
with automated quality tracking, logging, and notification capabilities.

Author: Data Team
Date: 2024
"""

import yaml
import os
import logging
import sys
import inspect
import time
import threading
import json
import zipfile
from functools import wraps
from datetime import datetime, timedelta

import boto3
import psutil
import requests
from pyspark.sql import functions as F, types as T, Window, DataFrame as SparkDataFrame

from src.utils import common as utils

# Configure logging
logger = logging.getLogger(__name__)



def get_param_from_func(func, args, kwargs, param_name):
    """
    Extract parameter value from function arguments using introspection.
    """
    # First check if parameter is in keyword arguments
    if param_name in kwargs:
        return kwargs[param_name]
    
    # If not in kwargs, check positional arguments using function signature
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())
    
    if param_name in params:
        idx = params.index(param_name)
        if len(args) > idx:
            return args[idx]
    
    return None


def parse_memory(mem_str):
    """
    Convert Spark memory configuration strings to float values in GB.   
    """
    if not mem_str:
        return None
        
    mem_str = mem_str.strip().lower()
    
    if mem_str.endswith('g'):
        return float(mem_str[:-1])
    elif mem_str.endswith('m'):
        return float(mem_str[:-1]) / 1024
    else:
        return None


def status_check(func):
    """
    Decorator for comprehensive job execution monitoring.
    
    This decorator wraps function execution with detailed tracking including:
    - Execution duration and status
    - Spark cluster configuration metrics
    - Error handling and logging
    - Resource usage tracking
    
    Args:
        func (callable): Function to be monitored
        
    Returns:
        callable: Wrapped function that returns execution metrics tuple
        
    The wrapped function returns:
        tuple: (update_time, run_date, status, duration, 
               executor_memory, executor_cores, num_executors,
               driver_memory, driver_cores)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract key parameters from function call
        run_date = get_param_from_func(func, args, kwargs, 'run_date')
        python_file = get_param_from_func(func, args, kwargs, 'python_file')
        spark = get_param_from_func(func, args, kwargs, 'spark')
        status = "RUNNING"

        # Initialize Spark resource metrics
        executor_memory = None
        executor_cores = None
        num_executors = None
        driver_memory = None
        driver_cores = None

        # Extract Spark configuration if available
        if spark is not None:
            try:
                conf = spark.sparkContext.getConf()
                executor_memory = parse_memory(conf.get("spark.executor.memory", "0G"))
                executor_cores = int(conf.get("spark.executor.cores", '1'))
                # Count active executors (excluding driver)
                num_executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keySet().size() - 1
                driver_memory = parse_memory(conf.get("spark.driver.memory", "0G"))
                driver_cores = int(conf.get("spark.driver.cores", '1'))
            except Exception as e:
                logger.warning(f"Failed to extract Spark configuration: {e}")
                print(f"Spark config extraction error: {e} -------------------")

        start_time = time.time()
        try:
            print(f"Running {func.__name__} for {run_date}")
            func(*args, **kwargs)
            status = "DONE"
        except Exception as e:
            print(f"Fail. Error: {e}")
            status = "FAIL"

        end_time = time.time()
        duration = end_time - start_time

        print(
            f"python_file: {python_file}, run_date: {run_date}, status: {status}, duration: {duration:.2f}s, "
            f"executor_memory: {executor_memory}, executor_cores: {executor_cores}, num_executors: {num_executors}, "
            f"driver_memory: {driver_memory}, driver_cores: {driver_cores} "
        )
        return (datetime.now(), run_date, status, duration, 
                executor_memory, executor_cores, num_executors,
                driver_memory, driver_cores)
    return wrapper


class JobQuality:
    """
    Main class for job execution monitoring and quality tracking.
    
    This class provides comprehensive monitoring capabilities for Spark-based
    ETL pipelines including period management, execution tracking, and S3 integration.
    
    Attributes:
        spark (SparkSession): Active Spark session
        log_path (str): S3 path for execution logs
        feature_dir (str): S3 path for feature data
        freq (str): Processing frequency ('daily' or 'weekly')
        date_from (str): Start date in YYYYMMDD format
        date_to (str): End date in YYYYMMDD format
        log_schema (StructType): Schema for execution logs
    """
    
    def __init__(self, spark, log_path, feature_dir, date_from, date_to, freq, start_time):
        """
        Initialize JobQuality instance.
        
        Args:
            spark (SparkSession): Active Spark session
            log_path (str): S3 path for execution logs
            feature_dir (str): S3 path for feature data
            date_from (str): Start date in YYYYMMDD format
            date_to (str): End date in YYYYMMDD format
            freq (str): Processing frequency ('daily' or 'weekly')
            
        Raises:
            ValueError: If freq is not 'daily' or 'weekly'
        """
        self.spark = spark
        self.log_path = log_path
        self.feature_dir = feature_dir
        self.save_dir = self.log_path + f"start_time={start_time}"

        if freq not in ("daily", "weekly"):
            raise ValueError("freq must be 'daily' or 'weekly'")
        
        self.freq = freq
        self.date_from = date_from
        self.date_to = date_to
        
        # Define schema for execution logs
        self.log_schema = T.StructType([
            T.StructField("feature_dir", T.StringType(), True),
            T.StructField("update_time", T.TimestampType(), True),
            T.StructField("run_date", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("duration", T.FloatType(), True),
            T.StructField("executor_memory", T.FloatType(), True),
            T.StructField("executor_cores", T.IntegerType(), True),
            T.StructField("num_executors", T.IntegerType(), True),
            T.StructField("driver_memory", T.FloatType(), True),
            T.StructField("driver_cores", T.IntegerType(), True),
        ])
        

    def check_feature_dir(self):
        """
        Scan S3 feature directory to discover existing data partitions (only in first run).
        
        This method lists all subdirectories in the feature directory and creates
        log entries for existing data partitions, marking them as "DONE".
        
        Returns:
            DataFrame: Spark DataFrame with discovered partitions and their status
        """
        config = utils.load_config("config_edit", "to_edit.yaml")

        # Initialize S3 client with credentials from config
        s3_client = boto3.client(
            's3', 
            aws_access_key_id=config['s3']['s3_access_key'], 
            aws_secret_access_key=config['s3']['s3_secret_key'], 
            endpoint_url=config['s3']['s3_endpoint']
        )
        
        # List objects in S3 bucket with pagination support
        response = s3_client.list_objects_v2(
            Bucket=config["s3"]["bucket_name"], 
            Prefix=self.feature_dir, 
            Delimiter="/"
        )
        
        # Extract directory names (date partitions) from S3 response
        # Assuming format: /path/to/feature/date=YYYYMMDD/
        length_prefix = len(self.feature_dir) + 5  # +5 for "date=" prefix
        list_in_dir = [i['Prefix'][length_prefix:-1] for i in response.get('CommonPrefixes', [])]

        # Handle paginated responses for large directories
        while response.get("IsTruncated"):
            continue_token = response['NextContinuationToken']
            response = s3_client.list_objects_v2(
                Bucket=config["s3"]["bucket_name"], 
                Prefix=self.feature_dir,
                Delimiter='/',
                ContinuationToken=continue_token
            )
            list_in_dir.extend([i['Prefix'][length_prefix:-1] for i in response.get('CommonPrefixes', [])])

        # Create log entries for discovered partitions
        periods_w_status = []
        for partition_date in list_in_dir:
            periods_w_status.append((
                self.feature_dir, 
                datetime.now(), 
                partition_date, 
                "DONE", 
                0.,  # duration
                0.,  # executor_memory
                0,   # executor_cores
                0,   # num_executors
                0.,  # driver_memory
                0    # driver_cores
            ))

        return self.spark.createDataFrame(periods_w_status, self.log_schema)
        

    def load_log_table(self):
        """
        Load existing execution logs or create new ones if none exist.
        
        This method attempts to load execution logs from S3. If no logs exist
        for the current feature directory, it falls back to scanning the S3
        directory structure to create initial log entries.
        
        Side Effects:
            Sets self.log_table attribute
            May save initial log data to S3 if none exists
        """
        try:
            # Attempt to load existing logs from S3
            log_table_full = utils.load_from_s3(self.spark, self.log_path)
            self.log_table = log_table_full.filter(F.col("feature_dir") == self.feature_dir)

            # If no logs exist for this feature, create them by scanning S3
            if self.log_table.count() == 0:
                print(f"JOB QUALITY LOG IS NONE! CHECKING FILE {self.feature_dir} .....")
                self.log_table = self.check_feature_dir()
                utils.save_to_s3(self.log_table, self.save_dir, mode='append')

        except Exception as e:
            logger.error(f"Failed to load log table: {e}")
            print(f"Creating log for {self.feature_dir}. It does not exist.")
            # Create new log table by scanning S3 directory
            self.log_table = self.check_feature_dir()
            utils.save_to_s3(self.log_table, self.save_dir)

    def parse_freq(self):
        """
        Returns:
            timedelta: Time interval based on frequency setting
                      - daily: 1 day
                      - weekly: 1 week
        """
        if self.freq == "daily":
            return timedelta(days=1)
        else:
            return timedelta(weeks=1)
        

    def adjust_start_date(self, date):
        """
        Adjust start date based on frequency requirements.
        
        For weekly frequency, adjusts the date to the next Monday if the
        provided date is not already a Monday.
        
        Args:
            date (str): Date string in YYYYMMDD format
        """
        date = datetime.strptime(date, "%Y%m%d")
        
        if self.freq == "daily":
            return date
        else:  # weekly frequency
            # If not Monday (weekday 0), move to next Monday
            if date.weekday() != 0:
                days_until_monday = 7 - date.weekday()
                date += timedelta(days=days_until_monday)
            return date

    def gen_periods(self, start_date, end_date):
        """
        Generate list of processing periods based on frequency.
        
        Args:
            start_date (str): Start date in YYYYMMDD format
            end_date (str): End date in YYYYMMDD format
            
        Returns:
            list: List of date strings in YYYYMMDD format
            
        Note:
            For daily frequency, excludes the end_date.
            For weekly frequency, generates Monday dates only.
        """
        start = self.adjust_start_date(start_date)
        end = datetime.strptime(end_date, "%Y%m%d")
        
        # For daily processing, exclude the end date
        if self.freq == 'daily':
            end = end - timedelta(days=1)
            
        periods = []
        while start <= end:
            periods.append(start.strftime("%Y%m%d"))
            start += self.parse_freq()
        
        return periods

    def get_periods_to_run(self, overwrite="false"):
        """
        Determine which periods need processing based on existing logs.
        
        Args:
            overwrite (str): Processing mode
                      - "false" (default): Incremental - only RUNNING/FAIL periods
                      - "true": All periods in date range
                      
        Returns:
            DataFrame: Spark DataFrame with run_date column containing
                      periods that need processing
        """
        # Ensure log table is loaded
        if not hasattr(self, "log_table") or self.log_table is None:
            self.load_log_table()

        # Generate all required periods for the date range
        all_periods_df = self.spark.createDataFrame(
            [(period,) for period in self.gen_periods(self.date_from, self.date_to)], 
            T.StructType([T.StructField("run_date", T.StringType(), True)])
        )
        
        # If overwrite mode, return all periods
        if overwrite == 'true':
            return all_periods_df.select('run_date').orderBy("run_date")
        
        # For incremental mode, find periods not yet completed
        # Use window function to get latest status per run_date
        window = Window.partitionBy("run_date").orderBy(F.col("update_time").desc())
        completed_df = self.log_table.withColumn("rn", F.row_number().over(window))\
            .filter(F.col("rn") == 1).filter(F.col("status") == "DONE")\
            .select("run_date")

        # Left anti join to find periods not completed
        periods_to_run = all_periods_df.join(completed_df, on='run_date', how="leftanti")

        return periods_to_run.select('run_date').orderBy("run_date")

    def append_log(self, log_data):
        """
        Append execution results to the log table in S3.
        """
        if not log_data:
            return
            
        log_df = self.spark.createDataFrame(log_data, self.log_schema)
        utils.save_to_s3(log_df, self.save_dir, mode='append')


def run_create_feature(func_and_kwargs, global_kwargs=None):
    """
    Main orchestration function for job execution pipeline.
    
    This function manages the complete job execution workflow including:
    - Configuration loading and validation
    - Period determination for incremental processing
    - Function chain execution with monitoring
    - Logging and notification handling
    
    Args:
        func_and_kwargs (list): List of (function, kwargs) tuples to execute
        global_kwargs (dict): Global configuration parameters containing:
                             - freq: Processing frequency ('daily' or 'weekly')
                             - feature_name: Feature identifier
                             - table_name: Source table configuration key
                             
    Command Line Args (from sys.argv[1]):
        python_file (str): Name of the executing Python file
        dag_name (str): DAG/workflow identifier  
        run_mode (str): Environment (prod/dev/test)
        dt_to (str): End date (YYYYMMDD)
        dt_from (str): Start date (YYYYMMDD)
        overwrite (str): Incremental or Overwrite ('false'|'true')
        start_time (str): Execution timestamp (YYYYMMDDHHMMSS)
    """
    # Parse command line arguments
    args = utils.process_args_to_dict(sys.argv[1])
    python_file = args['python_file']
    dag_name = args['dag_name']
    run_mode = args['run_mode']
    overwrite = args['overwrite']
    today = args['dt_to']
    if overwrite == 'true':
        dt_from = args['dt_from']
    else:
        dt_from = (datetime.strptime(today, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
    
    start_time = args['start_time']

    # Load configuration files
    common_config = utils.load_config("configs", "common.yaml")
    to_edit_config = utils.load_config("config_edit", "to_edit.yaml")

    # Extract job parameters from global configuration
    freq = global_kwargs['freq']
    feature_name = global_kwargs['feature_name']
    table_name = global_kwargs['table_name']

    # Set up output and log directories
    _outdir = f"{run_mode}/{common_config['outdir']['features']}" + f"{feature_name}/"
    _logpath = common_config['job_quality_log']

    # Initialize Spark session
    spark = utils.create_spark_instance(run_mode=run_mode, python_file=python_file)

    # Initialize job quality monitoring
    check_dir = utils.load_check_dir(feature_name, freq, common_config)
    log_job = JobQuality(
        spark=spark, 
        log_path=_logpath, 
        feature_dir=check_dir, 
        freq=freq, 
        date_to=today, 
        date_from=dt_from,
        start_time=start_time
    )
    
    # Determine periods that need processing
    periods = [row["run_date"] for row in log_job.get_periods_to_run(overwrite=overwrite).collect()]

    # Exit early if no periods need processing
    if not periods:
        utils.send_msg_to_telegram(
            f"IGNORE DUE TO ALL FILES UPDATED: {dag_name} | "
            f"node: {python_file} | update_time: {datetime.now().date()} | "
            f"run mode: {run_mode}"
        )
        return
    
    # Send start notification
    utils.send_msg_to_telegram(
        f"RUNNING: {dag_name} | node: {python_file} | "
        f"PERIODS: {periods} | run mode: {run_mode}"
    )
    
    # Process each period sequentially
    for run_date in periods:
        total_duration = 0.
        
        # Execute function chain for current period
        for func, kwargs in func_and_kwargs:
            # Inject configuration parameters if table_name is specified
            if table_name is not None:
                kwargs['common_config'] = common_config
                kwargs['run_mode'] = run_mode
                
                # Skip special handling for prepaid_danhba feature
                if feature_name != 'prepaid_danhba':
                    kwargs['table_name'] = to_edit_config['table_name'][table_name]
                    kwargs['bt_table_name'] = to_edit_config['backtest_table_dict'][table_name]['backtest_table_name']
                    kwargs['bt_msisdn_column'] = to_edit_config['backtest_table_dict'][table_name]['msisdn_column']
            
            # Execute function with status monitoring
            status_task = func(spark=spark, out_dir=_outdir, run_date=run_date, **kwargs)
            total_duration += status_task[3]  # Accumulate execution duration

        # Prepare status tuple for logging
        status = (check_dir,) + status_task[:3] + (total_duration,) + status_task[-5:]

        # Log execution results
        log_job.append_log([status])
        
        # Handle job failure
        if status[3] == "FAIL":
            raise RuntimeError("JOB FAILED!")   
        else:
            # Send success notification
            utils.send_msg_to_telegram(
                f"FINISH: {python_file} | run_date: {run_date} | "
                f"status: {status[3]} | duration: {status[4]/60:.4f}m | "
                f"run mode: {run_mode}"
            )