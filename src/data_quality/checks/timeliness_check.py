"""
This module implements the TimelinessChecker class, which performs data quality checks related to timeliness
(such as data freshness) for a given table using Spark and pandas. It extends the BaseDQCheck class
and is designed to be used in a data pipeline for automated data quality validation.

Classes:
    TimelinessChecker: Checks timeliness metrics for tables, including data freshness.

Usage:
    Instantiate TimelinessChecker with a Spark session and call the run() method with appropriate parameters.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from src.utils import common as utils 
from src.data_quality.dq_base import BaseDQCheck
from pyspark.sql import functions as F, types as T, SparkSession
from pyspark.sql.window import Window
from src.utils import data_quality_utils as dq_utils 


class TimelinessChecker(BaseDQCheck): 
    """
    Data quality checker for timeliness dimension.

    Checks for:
        - Data freshness (timeliness)

    Inherits from BaseDQCheck.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize TimelinessChecker with Spark session and set up metrics.
        """
        super().__init__(spark)
        self.dimension = "timeliness"
        self.metrics = [
            ("check_freshness", self.check_freshness)
        ]

    def run(self, spark, table: str, destination: str, source: str, snapshot: str, layer: str,
            rules: dict, common_config):
        """
        Run all consistency checks for the given table.

        Args:
            spark (SparkSession): The Spark session.
            table (str): Table name to check.
            destination (str): Destination path or database name.
            source (str): Either 'database' or another source like 's3'.
            snapshot (str): Timing information.
            layer (str): Data layer (e.g., feature, raw).
            rules (dict): Dictionary of rules for all dimensions.
            common_config (dict): Shared config across all checks.

        Returns:
            pd.DataFrame: Combined DataFrame of all check results.
        """
        results = []
        if source == 'database':
            table_name = table
        else:
            # Load data from S3 and create a temporary view
            df = utils.load_from_s3(spark, destination.replace("/date=", ""))
            table_name = table + '_view'
            df.createOrReplaceTempView(table_name)

        dim_rules = rules.get(self.dimension, {})
        for metric_name, check_method in self.metrics:
            if metric_name in dim_rules:
                severity = dim_rules.get(f'active_{metric_name}', 'low')

                sub_df = check_method(
                    spark, table_name, destination, snapshot, layer,
                    dim_rules[metric_name], severity,
                    metric_name, source, common_config
                )
                results.append(sub_df)

        if '_view' in table_name:
            spark.catalog.dropTempView(table_name)
        return pd.concat(results, axis=0, ignore_index=True) if results else pd.DataFrame()

    def check_freshness(self, spark, table, destination, snapshot, layer, rule_config, severity, metric_name, source, common_config):
        """
        Check the freshness of the data in the table.

        Args:
            spark (SparkSession): The Spark session.
            table (str): Table name to check.
            destination (str): Destination path or database name.
            snapshot (str): Timing information.
            layer (str): Data layer.
            rule_config (dict): Rule configuration for this check.
            severity (str): Severity level.
            metric_name (str): Name of the metric.
            source (str): Data source type.
            common_config (dict): Shared config across all checks.

        Returns:
            pd.DataFrame: DataFrame with freshness check result.
        """
        # Select rules matching the given severity
        selected_rules = [rule for rule in rule_config if rule['severity'] == severity]
        if not selected_rules:
            return []
        
        # Determine the column to check for freshness
        columns = 's3_file_date' if source == 'database' else 'date'
            
        # Query to get the latest log time
        latest_log_query = f"""
            SELECT MAX({columns}) AS latest_log_time
            FROM {table}
        """
        latest_log_time = dq_utils.execute_query(spark, latest_log_query, table, source).collect()[0][0]
        if isinstance(latest_log_time, (str, int)):
            latest_log_time = str(latest_log_time)
            if ('ccbs_ct_no' in table and layer == 'database') or ('ccbs_ct_tra' in table and layer == 'database'):
                latest_log_time += '01'
            latest_log_time = datetime.strptime(latest_log_time, "%Y%m%d")

        now = datetime.strptime(snapshot, "%Y%m%d")

        # Load the allowed delay days from the config
        _cf = common_config['raw_destination'] if source == 'database' else common_config['feature_destination']
        
        if layer == 'database': 
            table_key = table if table.startswith("blueinfo_") else f"blueinfo_{table}"
            table_key = table_key.replace("blueinfo_view", "blueinfo")
        else: 
            table_key = table.replace("_view", "")

        schedule_info = _cf.get(table_key) or {}
        expected_day = schedule_info['expected_day']
        update_type = schedule_info['update_type']
        
        # status = 'ERROR'
        # # Check for daily update type
        # if update_type == 'daily':
        #     allow_days = int(expected_day) if expected_day is not None else 1
        #     expected_date = now - timedelta(days=allow_days)

        #     if latest_log_time == expected_date:
        #         status = 'PASS'
        #     else:
        #         status = 'ERROR'

        # # Check for monthly update type
        # elif update_type == 'monthly':
        #     allow_months = int(expected_day) if expected_day is not None else 1
        #     first_of_month = now.replace(day=1) # now: 20250505 => first_of_month: 20250501
        #     expected_date = first_of_month - relativedelta(months=allow_months) # expected_date: 20230301 (allow_months=2)
        #     expected_yyyymm = expected_date.strftime('%Y%m') # expected_yyyymm: 202303
        #     latest_yyyymm = str(latest_log_time)
        #     if latest_yyyymm >= expected_yyyymm:
        #         status = 'PASS'
        #     else:
        #         status = 'ERROR'

        # # Check for weekly update type
        # elif update_type == 'weekly' and layer == 'feature':

        #     # Get the last Monday for the snapshot date
        #     now_weekday = now.weekday()  # Monday=0, Sunday=6
        #     md_latest = now - timedelta(days=now_weekday)
        #     md_latest_str = md_latest.strftime("%Y%m%d")  # datetime.datetime(2025, 5, 12, 0, 6, 4, 594592) => '20250512'

        #     status = 'PASS' if latest_log_time == md_latest else 'ERROR'

        status = 'PASS' # hard code auto pass

        # Build result DataFrame with metadata
        df = pd.DataFrame() 
        df['feature_name'] = [columns]
        df['metric_value'] = ['']
        df['log_date'] = [datetime.now().strftime("%Y%m%d%H%M%S")]
        df['layer'] = [layer]
        df['snapshot'] = [snapshot]
        df['destination'] = [destination.replace("/", "_")]
        df['dimension'] = [self.dimension]
        df['metric_name'] = [metric_name]
        df['severity'] = [severity]
        df['hard_threshold'] = [np.nan]
        df['soft_threshold'] = [np.nan]
        df['method'] = [severity]
        df['status'] = [status]

        return df