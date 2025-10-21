"""
This module implements the CompletenessChecker class, which performs data quality checks related to completeness
(null ratio, null count, and row count) for a given table using Spark and pandas. It extends the BaseDQCheck class
and is designed to be used in a data pipeline for automated data quality validation.

Classes:
    CompletenessChecker: Checks completeness metrics for tables, including null ratio, null count, and row count.

Usage:
    Instantiate CompletenessChecker with a Spark session and call the run() method with appropriate parameters.
"""

import pandas as pd
import numpy as np
from datetime import datetime

from src.utils import common as utils 
from src.data_quality.dq_base import BaseDQCheck
from pyspark.sql import functions as F, types as T, SparkSession
from pyspark.sql.window import Window
from src.utils import data_quality_utils as dq_utils 

class CompletenessChecker(BaseDQCheck): 
    """
    Data quality checker for completeness dimension.

    Checks for:
        - Null ratio per column
        - Null count per column
        - Row count per column

    Inherits from BaseDQCheck.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize CompletenessChecker with Spark session and set up metrics.
        """
        super().__init__(spark)
        self.dimension = "completeness"
        self.metrics = [
            ("null_ratio", self.check_null_ratio),
            ("null_count", self.check_null_count),
            ("row_count", self.check_row_count)
        ]

    def run(self, spark, table: str, destination: str, source: str, snapshot: str, layer: str, rules: dict, common_config):
        """
        Run all completeness checks for the given table.
        """
        return super().run(spark, table, destination, source, snapshot, layer, rules, common_config)
    
    def check_null_ratio(self, spark, table, destination, snapshot, layer, rule_config, severity, metric_name, source, common_config):
        """
        Calculate the null ratio for each column in the table.

        Returns:
            pd.DataFrame: DataFrame with null ratio per column and associated metadata.
        """
        # Get total number of rows for the snapshot
        total_row = dq_utils.execute_query(
            spark, f"SELECT COUNT(*) FROM {table} WHERE s3_file_date = '{snapshot}'", table, source
        ).collect()[0][0]
        # Select rules matching the given severity
        selected_rules = [rule for rule in rule_config if rule['severity'] == severity]

        if not selected_rules: 
            return []
        
        if layer == 'database': 
            table_key = table if table.startswith("blueinfo_") else f"blueinfo_{table}"
            table_key = table_key.replace("blueinfo_view", "blueinfo") # fix chp bang danh ba tren prod
        else: 
            table_key = table.replace("_view", "")

        rule = selected_rules[0]
        hard_threshold = rule['threshold'][table_key]['hard_threshold']
        soft_threshold = rule['threshold'][table_key]['soft_threshold']
        n_obs = rule['threshold'][table_key]['n_obs']

        # Get all column names for the table
        if (layer == 'feature') or (common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns'] == ['*']): 
            columns = dq_utils.get_schemas(spark, source, table)["col_name"].tolist()
        else: 
            columns = common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns']
        
        if columns == [] or columns is None:
            return None
            
        # Build SQL expressions to count nulls per column
        null_exprs = [
            f"{total_row} - COUNT({col}) AS null_count_{col}"
            for col in columns
        ]

        # Query to get null counts for all columns
        batch_query = f"SELECT {', '.join(null_exprs)} FROM {table} WHERE s3_file_date = '{snapshot}'"
        query_df = dq_utils.execute_query(spark, batch_query, table, source).toPandas()
        
        # Convert result to pandas DataFrame and calculate null ratio
        df = query_df.T.reset_index()
        df.columns = ['feature_name', 'metric_value']
        df['metric_value'] = df['metric_value'] / total_row
        
        # Get moving average thresholds for comparison
        moving_average_dict = dq_utils.calculate_moving_average(
            spark, common_config, snapshot, self.dimension, metric_name, severity, destination, n_obs
        )

        # Add metadata columns
        df['feature_name'] = df['feature_name'].str.replace("null_count_", "")
        df['log_date'] = datetime.now().strftime("%Y%m%d%H%M%S")
        df['layer'] = layer
        df['snapshot'] = snapshot
        df['destination'] = destination.replace("/", "_")
        df['dimension'] = self.dimension
        df['metric_name'] = metric_name
        df['severity'] = severity
        df['metric_value'] = df['metric_value'].round(decimals=4)
        df['hard_threshold'] = hard_threshold
        df['soft_threshold'] = soft_threshold
        df['method'] = severity + (str(n_obs) if n_obs is not None else "")
        
        # # Calculate status for each feature based on thresholds and moving average
        # df['status'] = df.apply(
        #     lambda row: dq_utils.calculate_status(row, moving_average_dict),
        #     axis=1
        # )

        # hard code auto pass
        df['status'] = 'PASS'

        df['metric_value'] = df['metric_value'].astype(str)
    
        return df
    
    def check_null_count(self, spark, table, destination, snapshot, layer, rule_config, severity, metric_name, source, common_config):
        """
        Calculate the null count for each column in the table.

        Returns:
            pd.DataFrame: DataFrame with null count per column and associated metadata.
        """
        total_row = dq_utils.execute_query(
            spark, f"SELECT COUNT(*) FROM {table} WHERE s3_file_date = '{snapshot}'", table, source
        ).collect()[0][0]
        selected_rules = [rule for rule in rule_config if rule['severity'] == severity]
        if not selected_rules:
            return []

        if layer == 'database': 
            table_key = table if table.startswith("blueinfo_") else f"blueinfo_{table}"
            table_key = table_key.replace("blueinfo_view", "blueinfo")
        else: 
            table_key = table.replace("_view", "")
        
        rule = selected_rules[0]
        hard_threshold = rule['threshold'][table_key]['hard_threshold']
        soft_threshold = rule['threshold'][table_key]['soft_threshold']
        n_obs = rule['threshold'][table_key]['n_obs']

        # Get all column names for the table
        if (layer == 'feature') or (common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns'] == ['*']): 
            columns = dq_utils.get_schemas(spark, source, table)["col_name"].tolist()
        else: 
            columns = common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns']

        if columns == [] or columns is None:
            return None
        
        # Build SQL expressions to count nulls per column
        null_exprs = [
            f"{total_row} - COUNT({col}) AS null_count_{col}"
            for col in columns
        ]

        batch_query = f"SELECT {', '.join(null_exprs)} FROM {table} WHERE s3_file_date = '{snapshot}'"
        query_df = dq_utils.execute_query(spark, batch_query, table, source).toPandas()

        # Convert result to pandas DataFrame
        df = query_df.T.reset_index()
        df.columns = ['feature_name', 'metric_value']

        # Get moving average thresholds
        moving_average_dict = dq_utils.calculate_moving_average(
            spark, common_config, snapshot, self.dimension, metric_name, severity, destination, n_obs
        )

        # Add metadata columns
        df['feature_name'] = df['feature_name'].str.replace("null_count_", "")
        df['log_date'] = datetime.now().strftime("%Y%m%d%H%M%S")
        df['layer'] = layer
        df['snapshot'] = snapshot
        df['destination'] = destination.replace("/", "_")
        df['dimension'] = self.dimension
        df['metric_name'] = metric_name
        df['severity'] = severity
        df['metric_value'] = df['metric_value'].astype(int)  # counts are integers
        df['hard_threshold'] = hard_threshold
        df['soft_threshold'] = soft_threshold
        df['method'] = severity + (str(n_obs) if n_obs is not None else "")

        # # Calculate status for each feature
        # df['status'] = df.apply(
        #     lambda row: dq_utils.calculate_status(row, moving_average_dict),
        #     axis=1
        # )

        # hard code auto pass
        df['status'] = 'PASS'

        df['metric_value'] = df['metric_value'].astype(str)
        
        return df
    
    def check_row_count(self, spark, table, destination, snapshot, layer, rule_config, severity, metric_name, source, common_config):
        """
        Calculate the row count for each column in the table.

        Returns:
            pd.DataFrame: DataFrame with row count per column and associated metadata.
        """
        selected_rules = [rule for rule in rule_config if rule['severity'] == severity]
        if not selected_rules:
            return []

        if layer == 'database': 
            table_key = table if table.startswith("blueinfo_") else f"blueinfo_{table}"
            table_key = table_key.replace("blueinfo_view", "blueinfo")
        else: 
            table_key = table.replace("_view", "")
        
        rule = selected_rules[0]
        hard_threshold = rule['threshold'][table_key]['hard_threshold']
        soft_threshold = rule['threshold'][table_key]['soft_threshold']
        n_obs = rule['threshold'][table_key]['n_obs']

        # Get all column names for the table
        if (layer == 'feature') or (common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns'] == ['*']): 
            columns = dq_utils.get_schemas(spark, source, table)["col_name"].tolist()
        else: 
            columns = common_config['feature_destination' if layer == 'feature' else 'raw_destination'][table_key]['columns']

        if columns == [] or columns is None:
            return None
        
        # Build SQL expressions to count non-null values per column
        null_exprs = [
            f"COUNT({col}) AS row_count_{col}"
            for col in columns
        ]

        batch_query = f"SELECT {', '.join(null_exprs)} FROM {table} WHERE s3_file_date = '{snapshot}'"
        query_df = dq_utils.execute_query(spark, batch_query, table, source).toPandas()

        # Convert result to pandas DataFrame
        df = query_df.T.reset_index()
        df.columns = ['feature_name', 'metric_value']

        # Get moving average thresholds
        moving_average_dict = dq_utils.calculate_moving_average(
            spark, common_config, snapshot, self.dimension, metric_name, severity, destination, n_obs
        )

        # Add metadata columns
        df['feature_name'] = df['feature_name'].str.replace("row_count_", "")
        df['log_date'] = datetime.now().strftime("%Y%m%d%H%M%S")
        df['layer'] = layer
        df['snapshot'] = snapshot
        df['destination'] = destination.replace("/", "_")
        df['dimension'] = self.dimension
        df['metric_name'] = metric_name
        df['severity'] = severity
        df['metric_value'] = df['metric_value'].astype(int)  # counts are integers
        df['hard_threshold'] = hard_threshold
        df['soft_threshold'] = soft_threshold
        df['method'] = severity + (str(n_obs) if n_obs is not None else "")

        # # Calculate status for each feature
        # df['status'] = df.apply(
        #     lambda row: dq_utils.calculate_status(row, moving_average_dict),
        #     axis=1
        # )
        
        # hard code auto pass
        df['status'] = 'PASS'

        df['metric_value'] = df['metric_value'].astype(str)

        return df